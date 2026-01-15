import os
import pickle
from glob import glob

from loguru import logger
from tqdm import tqdm

from tool.openai_api import get_embedding_batch
from tool.utils import init_chroma_client
from utils import read_json, save_to_pkl

COL_VEC_PKL_FILE = "database/cache_vector_db/spider_cols_tb_name_3_large.pkl"


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def make_col_embedding_text(d):
    """
    spider only has name, can only handle underscores..
    """
    col_name = d["name"].replace("_", " ").lower()

    # setting 1:
    # embedding_text = f"a column named {col_name}."

    # setting 2:
    t_name = d["table"].replace("_", " ").lower()
    embedding_text = f"a column named {col_name} in {t_name}."

    return embedding_text


def load_all_cols():
    paths = glob("database/cols_info/spider/*.json")
    paths.sort()

    cols = []
    for p in paths:
        cols.extend(read_json(p))
    return cols


def cache_cols_vec():
    cols = load_all_cols()
    ids, texts = [], []
    for d in cols:
        # id: db-table-col
        _id = f"{d['db']}@#@{d['table']}@#@{d['name']}"

        embedding_text = make_col_embedding_text(d)

        # DEBUG
        # logger.info(desc)

        ids.append(_id)
        texts.append(embedding_text)

    id_vecs = {}
    batch_size = 500  # Define your batch size here
    for batch_texts, batch_ids in tqdm(
        zip(chunks(texts, batch_size), chunks(ids, batch_size)),
        desc="getting embedding batch",
        total=len(texts) // batch_size + 1,
    ):
        batch_vec = get_embedding_batch(batch_texts, model="text-embedding-3-large")
        assert len(batch_vec) == len(batch_texts)
        for _id, vec in zip(batch_ids, batch_vec):
            id_vecs[_id] = vec

    save_to_pkl(id_vecs, COL_VEC_PKL_FILE)


def vectorization_cols():
    """
    return item: {"name": xxx, "type": "person", "desc": xxx}
    """
    cols = load_all_cols()
    id_vecs = pickle.load(open(COL_VEC_PKL_FILE, "rb"))

    ids, embeddings, metadatas, documents = [], [], [], []
    for d in cols:
        # id: db-table-col
        _id = f"{d['db']}@#@{d['table']}@#@{d['name']}"
        embedding_text = make_col_embedding_text(d)

        ids.append(_id)
        embeddings.append(id_vecs[_id])
        documents.append(embedding_text)

        # d: dict k:str(v)
        d = {k: str(v) for k, v in d.items()}
        metadatas.append(d)

    # chrom
    chroma_client = init_chroma_client(name="spider")
    index_name = os.path.basename(COL_VEC_PKL_FILE).replace(".pkl", "")
    if index_name in chroma_client.list_collections():
        chroma_client.delete_collection(index_name)
    collection = chroma_client.create_collection(index_name)
    collection.add(
        embeddings=embeddings,
        ids=ids,
        documents=documents,
        metadatas=metadatas,
    )
    count = collection.count()

    logger.info(f"Done {index_name}: {count}")


if __name__ == "__main__":
    # python preprocess/spider_db_vectorization.py

    # cache_cols_vec()
    vectorization_cols()
