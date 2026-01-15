import os
import sqlite3
import time
from glob import glob

from loguru import logger
from tqdm import tqdm

from tool.utils import get_foreign_keys, get_tables
from utils import read_json


def execute_with_retry(cursor, query, retries=10, delay=5):
    for attempt in range(retries):
        try:
            cursor.execute(query)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning(
                    f"Database is locked, retrying in {delay} seconds ({attempt + 1}/{retries})..."
                )
                time.sleep(delay)
            elif "unknown column" in str(e):
                logger.error(str(e))
            else:
                raise
    raise sqlite3.OperationalError(
        "Failed to acquire database lock after multiple retries."
    )


def commit_with_retry(conn, retries=10, delay=5):
    for _ in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to commit transaction: {e}")
            time.sleep(delay)
    raise sqlite3.OperationalError(
        "Failed to commit transaction after multiple retries."
    )


def add_foreign_keys_to_table(conn, table_name, foreign_keys):
    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION;")

    # Get existing table schema, including primary keys
    cursor.execute(f"PRAGMA table_info(`{table_name}`);")
    columns = cursor.fetchall()
    column_definitions = []
    primary_keys = []

    for col in columns:
        column_def = f"{col[1]} {col[2]}"
        if col[5]:  # if primary key
            primary_keys.append(col[1])
        if col[3]:  # if not null
            column_def += " NOT NULL"
        if col[4] is not None:  # if has default value
            column_def += f" DEFAULT {col[4]}"
        column_definitions.append(column_def)

    column_definitions_str = ", ".join(column_definitions)

    # Define primary keys constraint
    if primary_keys:
        pk_definitions = f", PRIMARY KEY({', '.join(primary_keys)})"
    else:
        pk_definitions = ""

    # Define foreign key constraints
    # foreign_keys: [{'field': 'oid', 'ref': 'author.oid'}]
    fk_definitions = ""
    for fk in foreign_keys:
        t2_name, t2_field = fk["ref"].split(".")
        fk_definitions += f"""
            FOREIGN KEY ({fk['field']}) REFERENCES `{t2_name}`({t2_field})
        """

    # Remove the last comma and space from fk_definitions
    fk_definitions = fk_definitions.strip()

    # Drop if exists
    execute_with_retry(cursor, f"DROP TABLE IF EXISTS `{table_name}_new`;")

    # Create a new temporary table with foreign keys and primary keys
    exe_sql = f"""
        CREATE TABLE `{table_name}_new` (
            {column_definitions_str}
            {pk_definitions},
            {fk_definitions}
        );
    """
    execute_with_retry(cursor, exe_sql)

    # Copy data from the existing table to the new table
    column_names = ", ".join([col[1] for col in columns])
    insert_sql = f"""
        INSERT INTO `{table_name}_new` ({column_names})
        SELECT {column_names} FROM `{table_name}`;
    """
    execute_with_retry(cursor, insert_sql)

    # Drop the existing table
    execute_with_retry(cursor, f"DROP TABLE `{table_name}`;")

    # Rename the new table to the original table name
    execute_with_retry(
        cursor, f"ALTER TABLE `{table_name}_new` RENAME TO `{table_name}`;"
    )

    # Commit the transaction
    commit_with_retry(conn)
    logger.info(f"Foreign keys added successfully to table `{table_name}`.")


def add_fk_to_db():
    """
    e.g.
    "bike_1": {
        "trip": {
            "zip_code": "weather.zip_code",
            "start_station_id": "station.id"
        }
    },
    means: in database bike_1, table trip, there are two missing foreign keys:
    - zip_code should reference weather.zip_code
    - start_station_id should reference station.id
    """
    missing = read_json("preprocess/spider_missing_fk.json")

    pbar = tqdm(missing.items(), ncols=100)
    for db, tables in missing.items():
        pbar.set_description(f"DB: {db}")
        db_path = f"dataset/spider/test_database/{db}/{db}.sqlite"

        conn = sqlite3.connect(db_path)

        for tb, fks in tables.items():
            foreign_keys = [{"field": field, "ref": ref} for field, ref in fks.items()]
            add_foreign_keys_to_table(conn, tb, foreign_keys)

        conn.close()
        pbar.update(1)


def find_fk_name():
    """
    Print foreign keys to check if the foreign key column names are the same
    """
    dbs = glob(f"dataset/spider/test_database/*")
    dbs = [p for p in dbs if os.path.isdir(p)]
    dbs = [os.path.basename(db) for db in dbs]
    dbs.sort()

    for db in dbs:
        db_path = f"dataset/spider/test_database/{db}/{db}.sqlite"

        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()

        tables = get_tables(cursor)
        for table in tables:
            foreign_keys = get_foreign_keys(cursor, table)
            for fk in foreign_keys:
                this_col, that_table, that_col = fk[3], fk[2], fk[4]
                if this_col != that_col:
                    print(
                        f"DB: {db}, Table: {table}, FK: {this_col} references {that_table}({that_col})"
                    )


if __name__ == "__main__":
    # python preprocess/spider_db_add_missing_fk.py

    add_fk_to_db()
    find_fk_name()
