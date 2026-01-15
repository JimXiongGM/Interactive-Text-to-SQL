import json
import os
import sqlite3
from glob import glob

import fire
from loguru import logger
from tqdm import tqdm

from tool.client_es import ESClient
from tool.utils import get_columns_types, get_tables, is_text_filed
from utils import read_json, save_to_json

es_client = ESClient()
mappings = {
    "properties": {
        "contents": {"type": "text"},
        "db": {"type": "keyword"},
        "table": {"type": "keyword"},
        "column": {"type": "keyword"},
    }
}


def index_data(split):
    assert split in ["train", "dev", "test"]
    paths = glob(f"dataset/bird/{split}_databases/*/*.sqlite")
    paths.sort()

    # for train, only keep the first 5 databases
    if split == "train":
        paths = paths[:5]

    logger.info(f"Total {len(paths)} databases.")

    has_text_filed_list = []

    pbar = tqdm(total=len(paths), ncols=100, colour="green")
    for db_path in paths:
        db = db_path.split("/")[-2]

        # debug
        # if db != "cre_Doc_Control_Systems":
        #     continue

        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        tables = get_tables(cursor)

        visited = set()

        es_idx = 0
        actions = []
        es_index = f"bird-{db}".lower()
        for t_idx, table in enumerate(tables):
            pbar.set_description(
                f"Processing DB: {db}, Table: {table}, {t_idx+1}/{len(tables)}"
            )
            columns_types = get_columns_types(cursor, table)
            for col, col_type in columns_types:
                # Note: Here we don't care if the text field is enumerable or not
                if is_text_filed(col_type):
                    cursor.execute(f"SELECT `{col}` FROM `{table}`")
                    rows = cursor.fetchall()
                    for row in rows:
                        if row[0]:
                            _key = f"{db}_{table}_{col}_{row[0]}"
                            if _key in visited:
                                continue
                            d = {
                                "id": es_idx,
                                "contents": row[0],
                                "_index": es_index,
                                "db": db,
                                "table": table,
                                "column": col,
                            }
                            actions.append(d)
                            es_idx += 1
                            visited.add(_key)
        pbar.update(1)
        if actions:
            has_text_filed_list.append(db)

        es_client.delete_index(es_index)
        es_client.create_index(es_index, mappings)
        es_client.bulk_insert(actions, thread_count=20)

    out_f = "preprocess/bird_db_has_text_filed_list.json"
    if os.path.exists(out_f):
        data = read_json(out_f)
        assert isinstance(data, list)
        has_text_filed_list.extend(data)
    with open(out_f, "w", encoding="utf-8") as f:
        has_text_filed_list = sorted(set(has_text_filed_list))
        json.dump(has_text_filed_list, f, ensure_ascii=False, indent=4)
    logger.info(f"Total {len(has_text_filed_list)} databases have text field.")


def test_search():
    db = "address"
    es_index = f"bird-{db}"
    queries = [{"match": {"contents": "Illinois"}}, {"match": {"contents": "Midwest"}}]
    res = es_client.msearch(es_index, queries)
    logger.info(res)


if __name__ == "__main__":
    # python preprocess/bird_db_indexing_es.py --split dev

    # index_data("train")
    # index_data("dev")
    # test_search()

    fire.Fire(index_data)
