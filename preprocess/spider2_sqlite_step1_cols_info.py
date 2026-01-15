import datetime
import os
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob

from loguru import logger
from tqdm import tqdm

from utils import save_to_json

os.makedirs("logs", exist_ok=True)
current_file_name = os.path.splitext(os.path.basename(__file__))[0]
current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
log_file_name = f"logs/{current_file_name}-{current_time}.log"
logger.add(log_file_name)

"""
ln -s ~/codes/Spider2/spider2-lite/resource/databases/[spider2-localdb, sqlite] ./dataset/
"""


def _single(_tuple):
    """
    Query min/max values in the database; one process per column in parallel
    return
    """
    db, table_name, col_info_db = _tuple
    index, col_name, data_format = col_info_db[0], col_info_db[1], col_info_db[2]

    item = {}
    item["index"] = index
    item["name"] = col_name
    item["data_format"] = data_format
    item["table"] = table_name
    item["db"] = db

    if table_name.lower() == "id":
        return item

    # Process data based on db
    db_file = f"dataset/spider2-sqlite/spider2-localdb/{db}.sqlite"
    conn = sqlite3.connect(db_file)
    conn.text_factory = lambda b: b.decode(errors="ignore")
    cursor = conn.cursor()

    # Check for empty table
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cursor.fetchone()[0]
    if count == 0:
        # logger.warning(f"Empty table `{table_name}` in db `{db}`.")
        # desc: Empty column
        item["statistics"] = "empty column."
        return item

    # add statistics.
    data_format = data_format.upper()
    if data_format == "TEXT" or "CHAR" in data_format or "VAR" in data_format:
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

                # For easier viewing
                unique_values.add(value)
                if len(unique_values) > 50:
                    is_text_field = True
                    break

        # Plain text (sentence): keep first 200 characters; enumerable: keep all
        if is_text_field:
            item["statistics"] = (
                "text filed, example: " + ", ".join(list(unique_values))[:100] + " ..."
            )
        else:
            item["statistics"] = dict(value_counts) if value_counts else "empty column."

    elif any([x in data_format for x in ["INTEGER", "INT", "ID"]]):
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`), COUNT(DISTINCT `{col_name}`) FROM `{table_name}`"
        )
        min_value, max_value, distinct_count = cursor.fetchone()
        if min_value is not None and max_value is not None:
            item["statistics"] = (
                f"min: {min_value}, max: {max_value}. distinct count: {distinct_count}"
            )
        else:
            item["statistics"] = f"dirty data, column value is none."
            logger.error(
                f"Invalid integer value. db: {db}, table: {table_name}, column: {col_name}"
            )
            return None

    elif any(
        [
            x in data_format
            for x in ["REAL", "NUMERIC", "DECIMAL", "NUMBER", "DOUBLE", "FLOAT", "NUM"]
        ]
    ):
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`) FROM `{table_name}`"
        )
        min_value, max_value = cursor.fetchone()
        if min_value is not None and max_value is not None:
            item["statistics"] = f"min: {min_value}, max: {max_value}"
        else:
            logger.error(
                f"Invalid real value. db: {db}, table: {table_name}, column: {col_name}"
            )
            return None

    elif any([x in data_format for x in ["DATE", "TIME", "YEAR"]]):
        cursor.execute(
            f"SELECT MIN(`{col_name}`), MAX(`{col_name}`) FROM `{table_name}`"
        )
        min_date, max_date = cursor.fetchone()
        if min_date and max_date:
            item["statistics"] = f"min: {min_date}, max: {max_date}"
        else:
            logger.error(
                f"Invalid date value. db: {db}, table: {table_name}, column: {col_name}"
            )
            return None

    elif "BOOL" in data_format:
        # count number of each value
        cursor.execute(
            f"SELECT `{col_name}`, COUNT(*) FROM `{table_name}` GROUP BY `{col_name}`"
        )
        value_counts = {}
        for row in cursor.fetchall():
            value_counts[row[0]] = row[1]
        item["statistics"] = (
            f"distinct count: {value_counts}" if value_counts else "empty column."
        )

    elif data_format == "BLOB":
        return None

    # Structured data, treat as text
    elif data_format in ["JSONB", "BLOB SUB_TYPE TEXT"]:
        cursor.execute(f"SELECT `{col_name}` FROM `{table_name}` LIMIT 1")
        value = cursor.fetchone()[0]
        item["statistics"] = f"structured data, example: {value}"

    else:
        logger.warning(
            f"Unknown data format: {data_format}, db: {db}, table: {table_name}, column: {col_name}"
        )
        # Unknown data format: JSONB, db: Airlines, table: aircrafts_data, column: model
        cursor.execute(f"SELECT `{col_name}` FROM `{table_name}` LIMIT 1")
        value = cursor.fetchone()[0]
        item["statistics"] = f"example value: {value}"
        x = 1

    return item


def gen_cols_info_from_db():
    """
    Only need to test
    List the row count for each table, alert empty tables and empty tables
    """
    db_infos = glob("dataset/spider2-sqlite/sqlite/*")
    db_infos = [p for p in db_infos if os.path.isdir(p)]

    db_files = glob(f"dataset/spider2-sqlite/spider2-localdb/*.sqlite")

    assert len(db_files) == len(
        db_infos
    ), f"db_files: {len(db_files)}, db_infos: {len(db_infos)}"

    data = []
    for db_file in db_files:
        db_name = os.path.basename(db_file).split(".")[0]

        # debug
        # if db_name not in ["complex_oracle","AdventureWorks","city_legislation",]:
        #     continue

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for item_tb in tables:
            table_name = item_tb[1]

            if table_name.lower() == "sqlite_sequence":
                continue

            # dataset/spider2-sqlite/sqlite/AdventureWorks/productmodelproductdescriptionculture.json
            # desc_file = f"dataset/spider2-sqlite/sqlite/{db_name}/{table_name}.json"
            # desc_info = read_json(desc_file)
            # col_desc = desc_info["description"]
            # Too strange, all desc are empty

            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            for col_info_db in cursor.fetchall():
                col_name = col_info_db[1]
                if "unnamed" in col_name.lower():
                    continue
                # col_desc = col_desc[col_name]

                data.append([db_name, table_name, col_info_db])  # , col_desc])

    logger.info(f"Total columns: {len(data)}")

    # multi
    new_data = []
    with ProcessPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(_single, item): item for item in data}
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            ncols=100,
            colour="green",
            desc="50 CPUs processing",
        ):
            result = future.result()
            if result is not None:
                new_data.append(result)
    logger.info(f"Total new data: {len(new_data)}")

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
        save_to_json(
            items, f"database/cols_info/spider2-sqlite/{db}.json", _print=False
        )


if __name__ == "__main__":
    """
    python preprocess/spider2_sqlite_tool_step1_get_all_cols.py
    """
    gen_cols_info_from_db()
