import functools
import json
import os
import pickle
import subprocess
import time
from concurrent import futures
from datetime import date, datetime
from glob import glob
from traceback import print_exc

from tqdm import tqdm


def read_json(path="test.json"):
    with open(path, "r", encoding="utf-8") as f1:
        res = json.load(f1)
    return res


def read_json_from_path(path_patten):
    paths = sorted(glob(path_patten))
    data = [
        read_json(i) for i in tqdm(paths, ncols=50, desc=f"Loading from {path_patten}")
    ]
    return data


def yield_json_from_path(path_patten):
    paths = sorted(glob(path_patten))
    data = (read_json(i) for i in paths)
    return data


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def _set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def save_to_json(obj, path, _print=True):
    if _print:
        print(f"SAVING: {path}")
    if type(obj) == set:
        obj = list(obj)
    dirname = os.path.dirname(path)
    if dirname and dirname != ".":
        os.makedirs(dirname, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f1:
        json.dump(
            obj,
            f1,
            ensure_ascii=False,
            indent=4,
            cls=ComplexEncoder,
            default=_set_default,
        )
    if _print:
        res = subprocess.check_output(f"ls -lh {path}", shell=True).decode(
            encoding="utf-8"
        )
        print(res)


def read_pkl(path="test.pkl"):
    with open(path, "rb") as f1:
        res = pickle.load(f1)
    return res


def save_to_pkl(obj, path, _print=True):
    dirname = os.path.dirname(path)
    if dirname and dirname != ".":
        os.makedirs(dirname, exist_ok=True)
    with open(path, "wb") as f1:
        pickle.dump(obj, f1)
    if _print:
        res = subprocess.check_output(f"ls -lh {path}", shell=True).decode(
            encoding="utf-8"
        )
        print(res)


def read_jsonl(path="test.jsonl", desc="", max_instances=None, _id_to_index_key=False):
    with open(path, "r", encoding="utf-8") as f1:
        res = []
        _iter = tqdm(enumerate(f1), desc=desc, ncols=150) if desc else enumerate(f1)
        for idx, line in _iter:
            if max_instances and idx >= max_instances:
                break
            res.append(json.loads(line.strip()))
    if _id_to_index_key:
        id_to_index = {i[_id_to_index_key]: idx for idx, i in enumerate(res)}
        return res, id_to_index
    else:
        return res


def save_to_jsonl(obj, path, _print=True):
    if isinstance(obj, set):
        obj = list(obj)
    elif isinstance(obj, dict):
        obj = obj.items()
    dirname = os.path.dirname(path)
    if dirname and dirname != ".":
        os.makedirs(dirname, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f1:
        for line in obj:
            f1.write(json.dumps(line, ensure_ascii=False) + "\n")
    if _print:
        res = subprocess.check_output(f"ls -lh {path}", shell=True).decode(
            encoding="utf-8"
        )
        print(res)


def get_filename(path):
    return os.path.splitext(os.path.basename(path))[0]


def time_now(fotmat="%Y-%m-%d %H:%M:%S"):
    date_time = datetime.now().strftime(fotmat)
    return date_time


def timestamp_now(level="ms"):
    t = time.time()
    if level == "ms":
        return int(round(t * 1000))
    elif level == "s":
        return int(t)
    else:
        return t


def format_time(times) -> str:
    desc = "second(s)"
    if times > 60:
        times = times / 60
        desc = "minute(s)"
    if times > 60:
        times = times / 60
        desc = "hour(s)"
    if times > 24:
        times = times / 24
        desc = "day(s)"
    return f"{times:.4f} {desc}"


def colorful(text, color="yellow"):
    if color == "yellow":
        text = "\033[1;33m" + str(text) + "\033[0m"
    elif color == "grey":
        text = "\033[1;30m" + str(text) + "\033[0m"
    elif color == "green":
        text = "\033[1;32m" + str(text) + "\033[0m"
    elif color == "red":
        text = "\033[1;31m" + str(text) + "\033[0m"
    elif color == "blue":
        text = "\033[1;94m" + str(text) + "\033[0m"
    else:
        pass
    return text


def timeout(seconds):
    executor = futures.ThreadPoolExecutor(1)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            future = executor.submit(func, *args, **kw)
            return future.result(timeout=seconds)

        return wrapper

    return decorator
