import os
from glob import glob

import fire
from loguru import logger

from tool.action_execution import chat_with_LLM
from utils import read_json


def load_schema_and_examples_dialog(dataset, add_evidence=False):
    """
    Return: List of str.
        [
            db1: {schema}\n\n{example1}\n\n{example2}\n\n...
            db2: ...
        ]
    """
    if "bird" in dataset.lower():
        if add_evidence:
            _ds = dataset + "-with_evidence"
        else:
            _ds = dataset + "-no_evidence"
    elif "spider2" in dataset:
        _ds = dataset
    else:
        _ds = dataset.split("-")[0]

    dbs = glob(f"prompt/{_ds}/*")
    dbs = [d for d in dbs if os.path.isdir(d)]
    dbs = [d.split("/")[-1] for d in dbs]
    logger.info(f"Loaded {len(dbs)} db examples.")

    prompts_by_db = []
    excludes = ["schema.txt", "test.txt"]
    for db in dbs:
        db_schema = open(f"prompt/{_ds}/{db}/schema.txt").read()
        paths = glob(f"prompt/{_ds}/{db}/*.txt")
        paths = [p for p in paths if not any([e in p for e in excludes])]
        paths.sort()
        examples = []

        for p in paths:
            _lines = open(p).readlines()
            lines = []
            for line in _lines:
                if line.startswith("#"):
                    continue
                lines.append(line)
            content = "".join(lines)

            # for spider2
            if "-- START --" in content:
                content = content.split("-- START --")[1]

            examples.append(content.strip())
        logger.info(f"Loaded {len(examples)} examples for {db}.")

        # "Table schema of database xx:"
        db_schema = f"Schema of database {db}:\n{db_schema.strip()}"
        examples = "\n\n".join(examples)
        db_str = f"{db_schema}\n\n{examples}"
        prompts_by_db.append(db_str)
    assert prompts_by_db, f"No examples found for {dataset}."
    return prompts_by_db


def load_tooldesc(dataset, add_evidence=False):
    if "bird" in dataset.lower():
        if add_evidence:
            _ds = dataset + "-with_evidence"
        else:
            _ds = dataset + "-no_evidence"
    else:
        _ds = dataset

    with open(f"prompt/{_ds}/tool_desc.txt") as f:
        tooldesc = f.read()
    return tooldesc.strip()


def load_test_data(dataset, add_evidence=False):
    """
    spider hardness = ['easy', 'medium', 'hard', 'extra']
    """
    if dataset == "bird-test":
        data = read_json(f"dataset/bird/test.json")
        for i in range(len(data)):
            data[i]["id"] = f"{data[i]['db_id']}-{data[i]['question_id']}"

    elif "spider2" in dataset:
        data = read_json(f"dataset/{dataset}.json")

    elif dataset == "spider2-lite-sqlite":
        data = read_json(f"dataset/spider2-lite-sqlite.json")

    else:
        _d = "spider" if "spider" in dataset else "bird"
        data = read_json(f"dataset_processed/{_d}/{dataset}.json")

    # if add_evidence, must be bird, skip "evidence"="".
    # solve these with no-evidence setting.
    if add_evidence:
        data = [d for d in data if d["evidence"]]

    logger.info(f"Loaded {len(data)} data.")
    return data


# ------------------------ main ------------------------ #


def run(dataset, model_name, debug=False, case_num=None, note="v1", add_evidence=False):
    """
    model_name: ["gpt-4-1106-preview", "llama2-7b-epoch3", ...]
    case_num = 200  # None: unlimit"train"  # "train":xx, "val":xx
    """
    assert dataset in [
        "spider-dev",
        "spider-test",
        "spider-dk",
        "spider-realistic",
        "spider-syn",
        "bird-dev",
        "bird-test",
        "bird-financial_corrected_sql",
        "bird-financial_corrected_data",
        "bird-minidev_sqlite",
        "spider2-lite-sqlite",
        "spider2-lite-snowflake",
    ]
    _dname = dataset.split("-")[0] if "spider2" not in dataset else dataset

    if add_evidence:
        logger.warning("Add evidence.")

    examplars = load_schema_and_examples_dialog(_dname, add_evidence=add_evidence)
    tooldesc = load_tooldesc(_dname, add_evidence=add_evidence)
    data = load_test_data(dataset, add_evidence=add_evidence)

    tooldesc_demos = tooldesc + "\n\n" + "\n\n".join(examplars)
    print(tooldesc_demos)

    data = data[:case_num]

    # debug
    # data = [i for i in data if i["id"] == "address-103"]

    # bird-dev-no_evidence or bird-dev-with_evidence
    if "bird" in dataset.lower():
        if add_evidence:
            _ds = dataset + "-with_evidence"
        else:
            _ds = dataset + "-no_evidence"
    else:
        _ds = dataset

    save_dir = f"save-crossdb-infer-dialog/{_ds}/{model_name}/{note}"
    logger.info(f"Saving to: {save_dir}")

    skip_ids = []
    if os.path.exists(save_dir):
        paths = glob(save_dir + "/*.json")
        skip_ids += [read_json(p)["id"] for p in paths]

    skip_ids = set(skip_ids)
    logger.info(f"Skip id: {len(skip_ids)}")
    data = [d for d in data if d["id"] not in skip_ids]
    logger.info(f"Remain data: {len(data)}")

    from concurrent.futures import ThreadPoolExecutor

    from tqdm import tqdm

    total_items = len(data)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in data:
            future = executor.submit(
                chat_with_LLM,
                d=item,
                model_name=model_name,
                save_dir=save_dir,
                tooldesc_demos=tooldesc_demos,
                max_round_num=12,
                dataset=dataset.split("-")[0] if "spider2" not in dataset else dataset,
                add_evidence=add_evidence,
            )
            futures.append(future)

        # Show progress bar
        for future in tqdm(
            futures, total=total_items, desc="Processing items", ncols=100
        ):
            try:
                future.result(timeout=300)  # 5 minutes timeout
            except Exception as e:
                logger.error(f"Error processing item: {e}")
                continue


def is_vscode_debug_mode():
    import sys

    return sys.breakpointhook != sys.__breakpointhook__


if __name__ == "__main__":
    """
    # ---------------------------- spider ---------------------------- #
    spider-dev(1034)/test(2147)
    spider-DK(535)
    spider-Realistc(508)
    spider-Syn(1034)

    # gpt-4o-2024-05-13
    python interactive_text_to_sql.py --dataset "spider-dev" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "spider-test" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "spider-dk" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "spider-realistic" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "spider-syn" --model_name "gpt-4o-2024-05-13" --debug True

    # ---------------------------- bird ---------------------------- #
    bird-dev(1534)

    python interactive_text_to_sql.py --dataset "bird-dev" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "bird-dev" --model_name "gpt-4o-2024-05-13" --debug True --add_evidence True

    python interactive_text_to_sql.py --dataset "bird-minidev_sqlite" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "bird-minidev_sqlite" --model_name "gpt-4o-2024-05-13" --debug True --add_evidence True

    # ---------------------------- bird financial (ACL 2024) ---------------------------- #
    # Understanding the Effects of Noise in Text-to-SQL- An Examination of the BIRD-Bench Benchmark
    (106)

    python interactive_text_to_sql.py --dataset "bird-financial_corrected_sql" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "bird-financial_corrected_sql" --model_name "gpt-4o-2024-05-13" --debug True --add_evidence True

    python interactive_text_to_sql.py --dataset "bird-financial_corrected_data" --model_name "gpt-4o-2024-05-13" --debug True
    python interactive_text_to_sql.py --dataset "bird-financial_corrected_data" --model_name "gpt-4o-2024-05-13" --debug True --add_evidence True

    # ---------------------------- spider2 ---------------------------- #
    spider2-lite-sqlite(135)
    spider2-lite-snowflake(198)

    python interactive_text_to_sql.py --dataset "spider2-lite-sqlite" --model_name "gpt-4o-2024-05-13"
    python interactive_text_to_sql.py --dataset "spider2-lite-snowflake" --model_name "gpt-4o-2024-05-13"
    """
    if is_vscode_debug_mode():
        logger.warning("Run in debug mode.")
        # run(dataset="bird-dev", model_name="meta-llama/Meta-Llama-3.1-8B-Instruct", debug=True, case_num=None, note="v1")
        run(
            dataset="spider2-lite-sqlite",
            model_name="gpt-4o-2024-05-13",
            debug=True,
            case_num=11,
            note="debug",
        )
    else:
        logger.info("Run in normal mode.")
        fire.Fire(run)
