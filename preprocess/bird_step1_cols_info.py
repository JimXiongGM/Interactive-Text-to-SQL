import datetime
import os
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob

import chardet
import fire
from loguru import logger
from tqdm import tqdm

from utils import read_json, save_to_json

os.makedirs("logs", exist_ok=True)
current_file_name = os.path.splitext(os.path.basename(__file__))[0]
current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
log_file_name = f"logs/{current_file_name}-{current_time}.log"
logger.add(log_file_name)

"""
for column_meaning.json.
"""


def detect_encoding(filename):
    with open(filename, "rb") as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    return result["encoding"]


def _single(_tuple):
    """
    Collect statistics for each field
    """
    split, db, table_name, col_info_db, column_desription = _tuple
    index, col_name, data_format = col_info_db[0], col_info_db[1], col_info_db[2]

    item = {}
    item["index"] = index
    item["name"] = col_name
    item["data_format"] = data_format
    item["table"] = table_name
    item["db"] = db
    item["column_description"] = column_desription

    if table_name.lower() == "id":
        return item

    # Process col_infos based on db
    db_file = f"dataset/bird/{split}_databases/{db}/{db}.sqlite"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    if data_format.upper() == "TEXT":
        query = f"SELECT `{col_name}` FROM `{table_name}`"
        cursor.execute(query)
        column_data = cursor.fetchall()

        # TEXT fields may contain enumerable values or sentences
        value_counts = defaultdict(int)
        unique_values = set()
        is_text_field = False

        for row in column_data:
            value = row[0]
            if value is not None:
                # Check if value is a sentence; if so, skip
                if value.strip().count(" ") > 8:
                    is_text_field = True

                # value might be an enumerable value
                elif not is_text_field:
                    value_counts[value] += 1

                # guess.
                unique_values.add(value)
                if len(unique_values) > 50:
                    is_text_field = True
                    break

        # Plain text (sentence): keep first 200 characters; enumerable: keep all
        if is_text_field:
            item["statistics"] = "text filed. " + str(unique_values)[:200]
        else:
            item["statistics"] = dict(value_counts)

    elif data_format.upper() == "INTEGER":
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`), COUNT(DISTINCT `{col_name}`) FROM `{table_name}`"
        )
        min_value, max_value, distinct_count = cursor.fetchone()
        if min_value is not None and max_value is not None:
            item["statistics"] = (
                f"min: {min_value}, max: {max_value}. distinct count: {distinct_count}"
            )
        else:
            logger.error(f"Invalid integer value in column `{col_name}`, db `{db}`.")

    elif data_format.upper() == "REAL":
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`) FROM `{table_name}`"
        )
        min_value, max_value = cursor.fetchone()
        if min_value is not None and max_value is not None:
            item["statistics"] = f"min: {min_value}, max: {max_value}"
        else:
            logger.error(f"Invalid real value in column `{col_name}`, db `{db}`.")

    elif data_format.upper() == "DATE":
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`) FROM `{table_name}`"
        )
        min_date, max_date = cursor.fetchone()
        if min_date and max_date:
            item["statistics"] = f"min: {min_date}, max: {max_date}"
        else:
            logger.error(f"Invalid date found in column `{col_name}`, db `{db}`.")

    # Add null count.
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` IS NULL")
    null_count = cursor.fetchone()[0]
    item["null_count"] = null_count

    return item


def gen_cols_info_from_db(split="train"):
    if split == "train":
        # only process 5 dbs.
        dbs = ["address", "airline", "app_store", "authors"]
    else:
        dbs = glob(f"dataset/bird/{split}_databases/*")
        dbs = [p for p in dbs if os.path.isdir(p)]
        dbs = [os.path.basename(db) for db in dbs]
    dbs.sort()

    # key: db|table|column
    _column_meaning = read_json("dataset/bird/column_meaning.json")
    column_meaning = {}
    for _k in _column_meaning:
        db, table, column = _k.split("|")
        column_meaning.setdefault(db, {})
        column_meaning[db].setdefault(table, {})
        column_meaning[db][table][column] = _column_meaning[_k]

    col_infos = []
    for db in dbs:

        # if db!="card_games":
        #     continue

        db_path = f"dataset/bird/{split}_databases/{db}/{db}.sqlite"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for item_tb in tables:
            table_name = item_tb[1]

            # if table_name != "set_translations":
            #     continue

            if table_name.lower() == "sqlite_sequence":
                continue

            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            for col_info_db in cursor.fetchall():
                # col_name is col_info_db[1]
                if db not in column_meaning:
                    logger.error(f"db `{db}` not in column_meaning.")
                    column_desription = ""
                elif table_name not in column_meaning[db]:
                    logger.error(f"table `{table_name}` not in column_meaning[{db}].")
                    column_desription = ""
                elif col_info_db[1] not in column_meaning[db][table_name]:
                    logger.error(
                        f"column `{col_info_db[1]}` not in column_meaning[{db}][{table_name}]."
                    )
                    column_desription = ""
                else:
                    column_desription = column_meaning[db][table_name][col_info_db[1]]
                col_infos.append(
                    [split, db, table_name, col_info_db, column_desription]
                )

    logger.info(f"Total col_infos: {len(col_infos)}")

    # multi
    new_data = []
    with ProcessPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(_single, item): item for item in col_infos}
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            ncols=100,
            colour="green",
            desc="100 CPUs processing",
        ):
            result = future.result()
            if result is not None:
                new_data.append(result)
    logger.info(f"Total new col_infos: {len(new_data)}")

    # Store separately by database
    db_data = {}
    for d in new_data:
        db = d["db"]
        if db not in db_data:
            db_data[db] = []
        db_data[db].append(d)

    # sort by index
    for db, items in db_data.items():
        items.sort(key=lambda x: (x["table"], x["index"]))
        save_to_json(items, f"database/cols_info/bird/{db}.json", _print=False)


if __name__ == "__main__":
    """
    python preprocess/bird_tool_step1_get_all_cols_for_column_meaning.py --split=dev
    """
    # gen_cols_info_from_db(split="train")
    # gen_cols_info_from_db(split="test")
    fire.Fire(gen_cols_info_from_db)
