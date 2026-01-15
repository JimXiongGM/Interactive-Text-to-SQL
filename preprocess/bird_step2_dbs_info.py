import os
import sqlite3
from glob import glob

import fire
from tqdm import tqdm

from preprocess.spider_step2_dbs_info import generate_markdown_table


def run(split):
    if split == "train":
        dbs = ["address", "airline", "app_store", "authors"]
    else:
        dbs = glob(f"dataset/bird/{split}_databases/*")
        dbs = [p for p in dbs if os.path.isdir(p)]
        dbs = [os.path.basename(db) for db in dbs]
    dbs.sort()

    for db in tqdm(dbs, ncols=100, colour="green", desc=f"Processing {split}"):
        # assert not os.path.exists(f"database/dbs_info/bird/{db}.md"), f"{db} already exists."

        db_path = f"dataset/bird/{split}_databases/{db}/{db}.sqlite"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        markdown_table = generate_markdown_table(cursor)

        # save to dbs_info
        os.makedirs(f"database/dbs_info/bird", exist_ok=True)
        with open(f"database/dbs_info/bird/{db}.md", "w", encoding="utf-8") as f:
            f.write(markdown_table)

        cursor.close()
        conn.close()


if __name__ == "__main__":
    # python preprocess/bird_tool_step2_db_info.py --split test
    # run("train")
    # run("dev")
    fire.Fire(run)
