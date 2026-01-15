import json
import os
import sqlite3
import threading
from typing import List

import openai
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random_exponential

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", None)
assert (
    OPENAI_API_KEY is not None
), "OPENAI_API_KEY is None, use `export OPENAI_API_KEY=your_key` to set it."

OPENAI_EMBEDDING_MODELS = [
    "text-embedding-ada-002",
    "text-embedding-3-small",
    "text-embedding-3-large",
]

client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=10)


@retry(wait=wait_random_exponential(min=5, max=30), stop=stop_after_attempt(3))
def chatgpt(
    prompt="Hello!",
    system_content="You are an AI assistant.",
    messages=None,
    model=None,
    temperature=0,
    top_p=0.95,
    n=1,
    stop=None,  # ["\n"],
    max_tokens=256,
    presence_penalty=0,
    frequency_penalty=0,
    logit_bias={},
    **kwargs,
):
    assert model is not None, "model name is None"

    messages = (
        messages
        if messages
        else [
            {"role": "system", "content": system_content},
            {"role": "user", "content": prompt},
        ]
    )

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        top_p=top_p,
        n=n,
        stop=stop,
        max_tokens=max_tokens,
        presence_penalty=presence_penalty,
        frequency_penalty=frequency_penalty,
        logit_bias=logit_bias,
    )
    # content = response["choices"][0]["message"]["content"]
    response = json.loads(response.model_dump_json())
    return response


thread_local = threading.local()


def get_sqlite_client():
    if not hasattr(thread_local, "cache_sql_client"):
        os.makedirs("database/cache_vector_query", exist_ok=True)
        cache_db_path = "database/cache_vector_query/local_cache.db"
        thread_local.cache_sql_client = sqlite3.connect(cache_db_path)
        cursor = thread_local.cache_sql_client.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS vec_cache (
                name TEXT PRIMARY KEY,
                vec TEXT NOT NULL
            );"""
        )
        thread_local.cache_sql_client.commit()
    return thread_local.cache_sql_client


def get_vec_cache(name):
    sql_client = get_sqlite_client()
    cursor = sql_client.cursor()
    cursor.execute(
        """SELECT vec FROM vec_cache WHERE name=?;""",
        (name,),
    )
    res = cursor.fetchone()
    if res:
        return json.loads(res[0])
    return None


def insert_vec_cache(name, vec):
    sql_client = get_sqlite_client()
    cursor = sql_client.cursor()
    if get_vec_cache(name):
        return
    if not isinstance(vec, str):
        vec = json.dumps(vec)
    cursor.execute(
        """INSERT INTO vec_cache (name, vec) VALUES (?, ?);""",
        (name, vec),
    )
    sql_client.commit()


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def get_embedding(
    text: str,
    model="text-embedding-3-small",
) -> list[float]:
    assert (
        model in OPENAI_EMBEDDING_MODELS
    ), f"model {model} not in {OPENAI_EMBEDDING_MODELS}"
    text_unikey = text + model
    res = get_vec_cache(text_unikey)
    if res:
        assert type(res) == list
        return res
    res = client.embeddings.create(input=[text], model=model).data[0].embedding
    insert_vec_cache(text_unikey, res)
    return res


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def get_embedding_batch(
    texts: List[str],
    model="text-embedding-3-small",
) -> list[float]:
    assert (
        model in OPENAI_EMBEDDING_MODELS
    ), f"model {model} not in {OPENAI_EMBEDDING_MODELS}"
    unseen_texts: List[str] = []
    for text in texts:
        cache = get_vec_cache(text + model)
        if cache is None:
            unseen_texts.append(text)

    if unseen_texts:
        req = client.embeddings.create(input=unseen_texts, model=model)
        vec_batch = [i.embedding for i in req.data]
        assert len(vec_batch) == len(unseen_texts)
        for unseen_text, vec in zip(unseen_texts, vec_batch):
            insert_vec_cache(unseen_text + model, vec)

    res = [get_vec_cache(text + model) for text in texts]
    assert None not in res
    return res


chatgpt_tok = None


def chatgpt_tokenize(text):
    global chatgpt_tok
    if chatgpt_tok is None:
        import tiktoken

        chatgpt_tok = tiktoken.encoding_for_model("gpt-4o-2024-05-13")
    res = chatgpt_tok.encode(text)
    return res


embedding_tok = None


def embedding_tokenize(text):
    global embedding_tok
    if embedding_tok is None:
        import tiktoken

        embedding_tok = tiktoken.encoding_for_model("text-embedding-3-small")
    res = embedding_tok.encode(text)
    return res
