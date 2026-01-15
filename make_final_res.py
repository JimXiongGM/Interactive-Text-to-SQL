import json
import os
from glob import glob

from tool.utils import extract_last_valid_sql, rewrite_sql_rm_cast


def post_process_sql(sql):
    # dirty fix
    if "cast(" in sql.lower():
        sql = rewrite_sql_rm_cast(sql)
    sql = sql.replace(" * 1.0)", ")")

    return sql


def get_final_sql(d):
    pred_sql = extract_last_valid_sql(d["dialog"])
    pred_sql = post_process_sql(pred_sql)
    return pred_sql


def spider():
    """
    spider-dev, spider-test, spider-dk, spider-syn, spider-realistic

    python evaluation/spider_eval_ts_raw.py \
        --gold dataset/spider/dev_gold.sql \
        --pred results_baseline/DAIL-SQL/spider-dev/DAIL-SQL+GPT-4.txt \
        --db dataset/spider/database \
        --table dataset/spider/tables.json \
        --etype all

    for each dataset, make golden sql, pred sql.
    golden: result/spider/golden/dev.sql
    predict: result/spider/predict/dev.txt
    """
    for split in ["dev", "test", "dk", "syn", "realistic"]:
        _key = "question"
        if split == "dev":
            p = "dataset/spider/dev.json"
        elif split == "test":
            p = "dataset/spider/test_data/dev.json"
        elif split == "dk":
            p = "dataset/Spider-DK/Spider-DK.json"
        elif split == "syn":
            p = "dataset/Spider-Syn/dev.json"
            _key = "SpiderSynQuestion"
        elif split == "realistic":
            p = "dataset/Spider-Realistic/dev.json"

        data = json.load(open(p))
        # fix an error in DK.
        if split == "dk":
            for i in range(len(data)):
                if (
                    data[i]["question"]
                    == "What are the different names and ages of the students who do have pets?"
                ):
                    data[i][
                        "query"
                    ] = "SELECT DISTINCT T1.fname , T1.LName,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid"

        goldens = [d["query"].replace("\t", " ") + "\t" + d["db_id"] for d in data]

        questions = [d[_key] for d in data]

        # pred
        p1 = f"save-crossdb-infer-dialog/spider-{split}/gpt-4o-2024-05-13/v1"
        paths = glob(p1 + "/*.json")
        assert paths, f"No data found in {p1}"
        pred_q_sql = {}
        for p in paths:
            d = json.load(open(p))
            pred_q_sql[d["question"]] = get_final_sql(d)
        pred_sqls = [pred_q_sql[q] for q in questions]
        assert len(goldens) == len(pred_sqls), f"{len(goldens)} {len(pred_sqls)}"

        # save
        os.makedirs(f"result/spider/golden", exist_ok=True)
        os.makedirs(f"result/spider/predict", exist_ok=True)
        with open(f"result/spider/golden/{split}.sql", "w", encoding="utf-8") as f:
            f.write("\n".join(goldens))
        with open(f"result/spider/predict/{split}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(pred_sqls))


def bird():
    """
    python -u evaluation/bird_evaluation_raw.py \
        --db_root_path ./dataset/bird/dev_databases/ \
        --ground_truth_path dataset/bird/dev.sql \
        --predicted_sql_path ${PWD}/results_baseline/BIRD-dev-baseline/predict_dev_haskg.json \
        --diff_json_path ./dataset/bird/dev.json \
        --num_cpus 20

    predict_dev_haskg.json:
    {
        "0": "SELECT xxx;\t----- bird -----\tcalifornia_schools",
        ...
    }
    Out:
    golden: result/bird/golden/dev.sql
    predict: result/bird/predict/with_evidence/dev.json
    """
    # bird-dev.json  bird-financial_corrected_data.json  bird-financial_corrected_sql.json  bird-minidev_sqlite.json
    for split in [
        "dev",
        "financial_corrected_data",
        "financial_corrected_sql",
        "financial_corrected_original",
        "minidev_sqlite",
    ]:
        if split == "dev":
            p = "dataset/bird/dev.json"
        elif split == "financial_corrected_original":
            p = "../the-effects-of-noise-in-text-to-SQL/datasets/financial.json"
        elif split == "financial_corrected_data":
            p = "../the-effects-of-noise-in-text-to-SQL/datasets/financial_corrected.json"
        elif split == "financial_corrected_sql":
            p = "../the-effects-of-noise-in-text-to-SQL/datasets/financial_corrected_sql.json"
        elif split == "minidev_sqlite":
            p = "dataset/bird-minidev/mini_dev_sqlite.json"

        data = json.load(open(p))
        goldens = [
            d["SQL"].replace("\t", " ").replace("\n", " ") + "\t" + d["db_id"]
            for d in data
        ]
        questions = [d["question"] for d in data]
        os.makedirs(f"result/bird/golden", exist_ok=True)
        with open(f"result/bird/golden/{split}.sql", "w", encoding="utf-8") as f:
            f.write("\n".join(goldens))

        # pred
        for setting in ["with_evidence", "no_evidence"]:
            p1 = (
                f"save-crossdb-infer-dialog/bird-{split}-{setting}/gpt-4o-2024-05-13/v1"
            )
            paths = glob(p1 + "/*.json")
            assert paths, f"No data found in {p1}"
            pred_q_sql = {}
            for p in paths:
                d = json.load(open(p))
                pred_q_sql[d["question"]] = (
                    extract_last_valid_sql(d["dialog"])
                    + "\t----- bird -----\t"
                    + d["db_id"]
                )

            pred_data = {idx: pred_q_sql[q] for idx, q in enumerate(questions)}
            assert len(goldens) == len(pred_data), f"{len(goldens)} {len(pred_data)}"

            # save
            os.makedirs(f"result/bird/predict/{setting}", exist_ok=True)
            with open(
                f"result/bird/predict/{setting}/{split}.json", "w", encoding="utf-8"
            ) as f:
                json.dump(pred_data, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    # python make_final_res.py
    spider()
    bird()
