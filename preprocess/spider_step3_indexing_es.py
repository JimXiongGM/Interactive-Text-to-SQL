import json
import sqlite3
from glob import glob

from loguru import logger
from tqdm import tqdm

from tool.client_es import ESClient
from tool.utils import get_columns_types, get_tables, is_text_filed

"""
ES Support Matrix: https://www.elastic.co/cn/support/matrix#matrix_jvm
Use: ES 8.14.1 + JDK 21

Download JDK 21: 
wget -c https://download.oracle.com/java/21/archive/jdk-21.0.2_linux-x64_bin.tar.gz
tar -xvzf jdk-21.0.2_linux-x64_bin.tar.gz -C ~/opt/
export ES_JAVA_HOME=~/opt/jdk-21.0.2

Download ES 8.14.1:
wget -c https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.14.1-linux-x86_64.tar.gz
tar -xvzf elasticsearch-8.14.1-linux-x86_64.tar.gz -C ~/opt/
mv ~/opt/elasticsearch-8.14.1 ~/opt/elasticsearch

cd ~/opt/elasticsearch

add to config/elasticsearch.yml:
path.data: data
path.logs: logs
network.host: 0.0.0.0
http.port: 9555

modify config/jvm.options:
-Xms8g
-Xmx8g

./bin/elasticsearch
save to ~/.es_config.json:
{"ca_certs":"/home/xionggm/opt/elasticsearch/config/certs/http_ca.crt","pwd":"xxx"}
"""

es_client = ESClient()
# contents,db,table,column
mappings = {
    "properties": {
        "contents": {"type": "text"},
        "db": {"type": "keyword"},
        "table": {"type": "keyword"},
        "column": {"type": "keyword"},
    }
}


def index_data():
    paths = glob("dataset/spider/test_database/*/*.sqlite")
    paths.sort()
    logger.info(f"Total {len(paths)} databases.")

    # for spyder-DK: new db: new_concert_singer, new_orchestra, new_pets_1
    for n in ["new_concert_singer", "new_orchestra", "new_pets_1"]:
        assert any([n in p for p in paths])

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

        es_idx = 0
        actions = []
        es_index = f"spider-{db}".lower()
        for t_idx, table in enumerate(tables):
            pbar.set_description(
                f"Processing DB: {db}, Table: {table}, {t_idx+1}/{len(tables)}"
            )
            columns_types = get_columns_types(cursor, table)
            for col, col_type in columns_types:
                if is_text_filed(col_type):
                    cursor.execute(f"SELECT `{col}` FROM `{table}`")
                    rows = cursor.fetchall()
                    for row in rows:
                        if row[0]:
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
        pbar.update(1)
        if actions:
            has_text_filed_list.append(db)

        es_client.delete_index(es_index)
        es_client.create_index(es_index, mappings)
        es_client.bulk_insert(actions, thread_count=20)

    with open(
        "preprocess/spider_db_has_text_filed_list.json", "w", encoding="utf-8"
    ) as f:
        json.dump(has_text_filed_list, f, ensure_ascii=False, indent=4)


def test_search():
    db = "activity_1"
    es_index = f"spider-{db}"
    # res = es_client.search(es_index, query={"match": {"contents": "david"}})
    # print(res)

    queries = [
        {"index": es_index, "query": {"match": {"contents": "Canoeing"}}},
        {"index": es_index, "query": {"match": {"contents": "gabay"}}},
    ]

    body = []
    for query in queries:
        body.append({"index": query["index"]})
        body.append({"query": query["query"]})

    res = es_client.client.msearch(body=body)
    responses = res.body["responses"]
    print(res)

    queries = [{"match": {"contents": "david"}}, {"match": {"contents": "gabay"}}]
    res = es_client.msearch(es_index, queries)
    print(res)


if __name__ == "__main__":
    # python preprocess/spider_db_indexing_es.py

    # index_data()
    test_search()
