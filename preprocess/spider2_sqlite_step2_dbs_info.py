import os
import sqlite3
from glob import glob

from tqdm import tqdm

from tool.utils import get_foreign_keys, get_primary_key, get_tables


def generate_markdown_table(cursor):
    tables = get_tables(cursor)
    markdown = "| Table | Primary Key | Foreign Key | Row Count | Column Count |\n"
    markdown += "| --- | --- | --- | --- | --- |\n"

    for table in sorted(tables):
        primary_keys = get_primary_key(cursor, table)
        foreign_keys = get_foreign_keys(cursor, table)

        primary_key_str = ", ".join(primary_keys)

        foreign_key_str = ""
        if foreign_keys:
            foreign_key_str = ", ".join(
                [f"{fk[3]} references {fk[2]}({fk[4]})" for fk in foreign_keys]
            )

        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        row_count = cursor.fetchone()[0]

        cursor.execute(f"PRAGMA table_info(`{table}`)")
        info = cursor.fetchall()
        column_count = len(info)

        markdown += f"| {table} | {primary_key_str} | {foreign_key_str} | {row_count} | {column_count} |\n"

    return markdown


def run():
    paths = glob("dataset/spider2-sqlite/spider2-localdb/*.sqlite")
    paths.sort()

    for db_path in tqdm(paths, desc="Generating DB info", ncols=100):
        db_name = os.path.basename(db_path).split(".")[0]

        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        markdown_table = generate_markdown_table(cursor)

        # save to dbs_info
        os.makedirs(f"database/dbs_info/spider2-sqlite", exist_ok=True)
        with open(
            f"database/dbs_info/spider2-sqlite/{db_name}.md", "w", encoding="utf-8"
        ) as f:
            f.write(markdown_table)

        cursor.close()
        conn.close()


if __name__ == "__main__":
    # python preprocess/spider2_sqlite_step2_db_info.py
    run()
