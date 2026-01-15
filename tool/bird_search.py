import os
from glob import glob
from typing import List, Union

from loguru import logger

from tool.openai_api import get_embedding_batch
from tool.spider_search import GraphSearcher
from tool.utils import init_chroma_client, is_dict
from utils import read_json, timeout

COL_VEC_PKL_FILE = "database/cache_vector_db/bird_cols_tb_name_coldesc_3_large.pkl"


def load_bird_dbs():
    paths = glob("database/cols_info/bird/*.json")
    paths.sort()

    dbs = [os.path.basename(p).replace(".json", "") for p in paths]
    dbs.sort()
    return dbs


def get_sqlite_file(db):
    p1 = f"dataset/bird/train_databases/{db}/{db}.sqlite"
    p2 = f"dataset/bird/dev_databases/{db}/{db}.sqlite"
    if os.path.exists(p1):
        return p1
    elif os.path.exists(p2):
        return p2
    else:
        raise FileNotFoundError(f"Database {db} not found in train or dev databases.")


# -------------------------- search column -------------------------- #


def create_column_searcher(db):
    """
    column_searcher = create_column_searcher("your_db_name", "path_to_col_vec_pkl_file")
    results = column_searcher("your_query", topk=5)
    print(results)
    """

    dbs = load_bird_dbs()
    assert db in dbs, f"db `{db}` not found in {dbs}"

    chroma_client = init_chroma_client("bird")
    index_name = os.path.basename(COL_VEC_PKL_FILE).replace(".pkl", "")
    collection = chroma_client.get_collection(index_name)

    @timeout(60)
    def search_column(query: Union[str, List[str]], topk=5, str_mode=True):
        """
        str_mode: real use. each item one line.
        query: [q1, q2, ...]
        Return: {q1: [x1,x2,...], q2: [x1,x2,...], ...}
        """
        query = query if isinstance(query, list) else [query]
        results = {}
        for q in query:
            result = []
            q = q.strip().lower()

            # make desc of column
            desc = f"a column about {q}."

            # get embedding
            vec = get_embedding_batch([desc], model="text-embedding-3-large")
            result = collection.query(
                query_embeddings=vec[0], n_results=max(10, topk), where={"db": db}
            )

            # add distance to metadata
            format_results = []
            for metadata, distance in zip(
                result["metadatas"][0], result["distances"][0]
            ):
                # dict_keys(['column_description', 'data_format', 'db', 'index', 'name', 'name_csv', 'table', 'value_description', 'statistics'])
                # print(metadata.keys())

                # print key by: name, table, column_description, data_format, statistics, distance
                new_metadata = {
                    "column": metadata["name"],
                    "format": metadata["data_format"],
                    "table": metadata["table"],
                    # "column_description": metadata["column_description"],
                }

                if "value_description" in metadata and metadata["value_description"]:
                    new_metadata["value_description"] = metadata["value_description"]

                if "statistics" in metadata:
                    _vd = metadata["statistics"]
                    if is_dict(_vd):
                        _vd = list(eval(_vd).keys())
                        _vd = f"categorical field. {_vd}"
                        if len(_vd) > 400:
                            _vd = _vd[:400] + f"...(Omit {len(_vd) - 400} chars)"
                    elif len(_vd) > 200:
                        _vd = _vd[:200] + f"...(Omit {len(_vd) - 200} chars)"
                    new_metadata["statistics"] = _vd

                # fix weird bug
                if q == metadata["name"].lower():
                    distance = 0.0
                new_metadata["distance"] = distance
                format_results.append(new_metadata)

            format_results.sort(key=lambda x: x["distance"])

            # If distance==0 > topk, topk = Num(distance==0)
            _topk = max(topk, len([i for i in format_results if i["distance"] == 0.0]))
            if _topk != topk:
                # print DB and query info
                logger.warning(
                    f"DB: `{db}`, Query: `{q}`, Topk: {topk}, Num(distance==0): {_topk}"
                )
                topk = _topk

            if str_mode:
                for r in format_results[:topk]:
                    r.pop("distance")
            results[q] = format_results[:topk]

        if len(results) == 1:
            results = results[query[0].lower()]
        if str_mode:
            results = str(results)
        return results

    return search_column


# -------------------------- search path -------------------------- #


class GraphSearcherBIRD(GraphSearcher):
    def __init__(
        self,
    ) -> None:
        # dataset/bird/test_database/aan_1/aan_1.sqlite
        self.dbs_name = load_bird_dbs()
        self._nx_cache_dir = ".cache/nx_cache/bird"
        self._nx_tmp_dir = ".tmp/nx/bird"
        os.makedirs(self._nx_cache_dir, exist_ok=True)
        os.makedirs(self._nx_tmp_dir, exist_ok=True)
        self._node_pair_to_edge = {}  # {db: {(n1,n2):edge, ...}, db2:...}
        self._G = {}  # {db: nx.Graph}

    def _get_sqlite_file(self, db: str):
        return get_sqlite_file(db)


g_searcher = GraphSearcherBIRD()


def create_path_finder(db: str):
    global g_searcher
    # for init
    g_searcher.find_shortest_path(db=db, start="!@#$%^", end="!@#$%^")

    @timeout(60)
    def _find_shortest_path(
        start: Union[str, List[str]], end: Union[str, List[str]], debug=False
    ):
        return g_searcher.find_shortest_path(db=db, start=start, end=end, debug=debug)

    return _find_shortest_path


# -------------------------- search value -------------------------- #


def create_value_searcher(db):
    """
    db_has_text_filed_list: Output by preprocess/bird_db_indexing_es.py
    """
    db_has_text_filed_list = read_json("preprocess/bird_db_has_text_filed_list.json")
    if db in db_has_text_filed_list:
        from tool.client_es import ESClient

        es_client = ESClient()
        FLAG_GO = True
    else:
        FLAG_GO = False

    @timeout(60)
    def _search(
        query: Union[str, List[str]],
        table: Union[str, List[str]] = [],
        column: Union[str, List[str]] = [],
        topk=5,
        str_mode=True,
    ):
        """
        query: [q1, q2, ...]
        Return: {q1: [x1,x2,...], q2: [x1,x2,...], ...}
        """
        if not FLAG_GO:
            return f"Error. Tables in db `{db}` do not have text field."

        if isinstance(query, str):
            query = [query]
        if isinstance(table, str):
            table = [table]
        if isinstance(column, str):
            column = [column]

        if table == []:
            table = [None] * len(query)
        if column == []:
            column = [None] * len(query)

        if not len(query) == len(table) == len(column):
            return "Error. Length of query, table, column should be the same."

        results = {}

        # queries = [{"match": {"contents": "david"}}, {"match": {"contents": "gabay"}}]
        queries = [{"match": {"contents": q}} for q in query]
        responses = es_client.msearch(
            index=f"bird-{db}".lower(), queries=queries, size=50
        )

        for q, res, t, c in zip(query, responses, table, column):
            hits = res["hits"]["hits"]
            result = []
            for hit in hits:
                doc = hit["_source"]
                if t and doc.get("table", None) != t:
                    continue
                if c and doc.get("column", None) != c:
                    continue
                doc["score"] = hit["_score"]
                doc.pop("id", None)
                result.append(doc)
            result = result[:topk]
            if str_mode:
                for r in result:
                    r.pop("score")
                    r.pop("db")
            results[q] = result

        if len(results) == 1:
            results = results[query[0]]

        return str(results) if str_mode else results

    return _search


if __name__ == "__main__":
    # python tool/bird_search.py

    # ---------- SearchColumn ----------
    # SearchColumn = create_column_searcher("card_games")
    # r = SearchColumn("language", topk=5)  # want: the abbreviation of the state
    # print(r)

    # SearchColumn = create_column_searcher("address")
    # r = SearchColumn(["house hold", "address code"], topk=5)  # want: the abbreviation of the state
    # print(r)

    # ---------- SearchPath ----------
    # start = "sets.name"
    # end = ["set_translations.language"]
    # path_finder = create_path_finder("card_games")
    # res = path_finder(start=start, end=end, debug=True)
    # print(res)

    # ---------- SearchValue ----------
    SearchValue = create_value_searcher("codebase_community")
    res = SearchValue(query="2010")
    print(res)

    # SearchValue = create_value_searcher("address")
    # res = SearchValue(["east setauk", "new york"])
    # print(res)
