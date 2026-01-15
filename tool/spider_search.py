import os
import pickle
import sqlite3
from glob import glob
from typing import List, Union

import networkx as nx
from loguru import logger

from tool.openai_api import get_embedding_batch
from tool.utils import get_foreign_keys, get_tables, init_chroma_client, is_dict
from utils import read_json

COL_VEC_PKL_FILE = "database/cache_vector_db/spider_cols_tb_name_3_large.pkl"


# -------------------------- search column -------------------------- #


def create_column_searcher(db):
    """
    column_searcher = create_column_searcher("your_db_name", "path_to_col_vec_pkl_file")
    results = column_searcher("your_query", topk=5)
    print(results)
    """
    paths = glob("dataset/spider/test_database/*")
    dbs = [os.path.basename(p).replace(".json", "") for p in paths]
    dbs.sort()
    assert db in dbs, f"db `{db}` not found in {dbs}"

    chroma_client = init_chroma_client(name="spider")
    index_name = os.path.basename(COL_VEC_PKL_FILE).replace(".pkl", "")
    collection = chroma_client.get_collection(index_name)

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

                if "value_description" in metadata:
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


class GraphSearcher:
    def __init__(self, database_dir: str) -> None:
        # dataset/spider/test_database/aan_1/aan_1.sqlite
        self.database_dir = database_dir
        paths = glob(f"{database_dir}/*")
        dbs_name = [p for p in paths if os.path.isdir(p)]
        dbs_name = [os.path.basename(db) for db in dbs_name]
        self.dbs_name = sorted(dbs_name)
        self._nx_cache_dir = ".cache/nx_cache/spider"
        self._nx_tmp_dir = ".tmp/nx/spider"
        os.makedirs(self._nx_cache_dir, exist_ok=True)
        os.makedirs(self._nx_tmp_dir, exist_ok=True)
        self._node_pair_to_edge = {}  # {db: {(n1,n2):edge, ...}, db2:...}
        self._G = {}  # {db: nx.Graph}

    def _make_node_pair(self, db: str, G: nx.Graph):
        if db not in self._node_pair_to_edge:
            self._node_pair_to_edge[db] = {}
            for edge in G.edges(data=True):
                start, end, data = edge
                self._node_pair_to_edge[db][(start, end)] = data["label"]
                self._node_pair_to_edge[db][(end, start)] = data["label"]

    def _get_sqlite_file(self, db: str):
        p = f"{self.database_dir}/{db}/{db}.sqlite"
        assert os.path.exists(p), f"Database {db} not found in {self.database_dir}"
        return p

    def _make_graph(self, db, force_tag=False):
        """
        Nodes: table, column
        Edges:
            1. in table: table -> column
            2. between tables: table -> ref_table
        """
        if db in self._G and not force_tag:
            return

        # load cache if exists
        if os.path.exists(f"{self._nx_cache_dir}/{db}.gpickle") and not force_tag:
            with open(f"{self._nx_cache_dir}/{db}.gpickle", "rb") as f:
                G = pickle.load(f)
            self._make_node_pair(db, G)
            self._G[db] = G

        conn = sqlite3.connect(self._get_sqlite_file(db))
        cursor = conn.cursor()

        edges = []

        tables = get_tables(cursor)
        nodes = tables.copy()
        for table in tables:
            # in table
            # [("cite", "cite.pid", "cite.pid"), ...]
            cursor.execute(f"PRAGMA table_info(`{table}`)")
            columns = cursor.fetchall()
            columns = [col[1] for col in columns]
            for column in columns:
                # column name should be "table.col"
                column_node = f"{table}.{column}"
                nodes.append(column_node)
                # edge: table -> column
                edges.append((table, column_node, f"Col: {column_node}"))

            # between tables
            # [("cite", "publication", "cite.pid = publication.pid"), ...]
            foreign_keys = get_foreign_keys(cursor, table)
            for fk in foreign_keys:
                # id, seq, table, from, to, on_update, on_delete, match
                ref_table_name, ref_column_name, this_column_name = fk[2], fk[3], fk[4]
                # edge: table -> ref_table
                edges.append(
                    (
                        table,
                        ref_table_name,
                        f"FK: {table}.{this_column_name} = {ref_table_name}.{ref_column_name}",
                    )
                )

        G = nx.MultiGraph()
        G.add_nodes_from(nodes)

        for start, end, label in edges:
            G.add_edge(start, end, label=label)

        with open(f"{self._nx_cache_dir}/{db}.gpickle", "wb") as f:
            pickle.dump(G, f, pickle.HIGHEST_PROTOCOL)
        self._make_node_pair(db, G)
        self._G[db] = G

    def find_shortest_path(
        self,
        db: str,
        start: Union[str, List[str]],
        end: Union[str, List[str]],
        debug=False,
    ):
        """
        return:
            [("t1.c1","t5.c5", " xx <-> xx"), ...]
        """
        assert db in self.dbs_name, f"Database {db} not found in {self.database_dir}"
        self._make_graph(db, force_tag=debug)

        if isinstance(start, str):
            start = [start]
        if isinstance(end, str):
            end = [end]

        start = sorted(set(start))
        end = sorted(set(end))

        res = []
        for s in start:
            for e in end:
                r = self._find_shortest_path(db, s, e, debug)
                res.append((s, e, r))
        if len(res) == 1:
            res = res[0][2]
        return res

    def _find_shortest_path(self, db: str, start: str, end: str, debug=False):
        G = self._G[db]
        if start not in G.nodes:
            err = f"Error. Node {start} not found in {db}."
            return err
        if end not in G.nodes:
            err = f"Error. Node {end} not found in {db}."
            return

        try:
            path = nx.shortest_path(G, source=start, target=end)
            node_pairs = [(path[i], path[i + 1]) for i in range(len(path) - 1)]
            edges = [self._node_pair_to_edge[db][node_pair] for node_pair in node_pairs]
            res = " <-> ".join(edges)
            res = res.replace("Col: ", "").replace("FK: ", "")
            # res = f"Shortest path from {start} to {end}: {' -> '.join(path)}"
        except nx.NetworkXNoPath:
            err = f"Error. No path between {start} and {end}."
            return err

        if debug:
            import matplotlib.pyplot as plt

            plt.figure(figsize=(20, 15), dpi=300)
            pos = nx.kamada_kawai_layout(G)
            nx.draw(
                G,
                pos,
                with_labels=True,
                node_size=1000,
                node_color="skyblue",
                font_size=10,
                font_weight="bold",
            )

            path_edges = set(zip(path, path[1:]))
            normal_edges = set(G.edges()) - path_edges

            nx.draw_networkx_edges(
                G, pos, edgelist=normal_edges, edge_color="black", alpha=0.5
            )

            nx.draw_networkx_edges(
                G, pos, edgelist=path_edges, edge_color="red", width=2.0
            )

            edge_labels = {}
            for u, v, data in G.edges(data=True):
                label = f"{data['label']}"
                if (u, v) in path_edges or (v, u) in path_edges:
                    edge_labels[(u, v)] = (label, "red")
                else:
                    edge_labels[(u, v)] = (label, "black")

            for (u, v), (label, color) in edge_labels.items():
                nx.draw_networkx_edge_labels(
                    G, pos, edge_labels={(u, v): label}, font_size=8, font_color=color
                )

            plt.title(f"Shortest Path from `{start}` to `{end}`")
            _save_p = f"{self._nx_tmp_dir}/__{db}__-__{start}__-__{end}__.png"
            plt.savefig(_save_p)
            plt.close()
            logger.info(f"Shortest path image saved to {_save_p}")

        return res


g_searcher = GraphSearcher(database_dir="dataset/spider/test_database")


def create_path_finder(db: str):
    global g_searcher
    g_searcher.find_shortest_path(db=db, start="!@#$%^", end="!@#$%^")

    def _find_shortest_path(
        start: Union[str, List[str]], end: Union[str, List[str]], debug=False
    ):
        return g_searcher.find_shortest_path(db=db, start=start, end=end, debug=debug)

    return _find_shortest_path


# -------------------------- search value -------------------------- #


def create_value_searcher(db):
    """
    db_has_text_filed_list: Output by preprocess/spider_db_indexing_es.py
    """
    db_has_text_filed_list = read_json("preprocess/spider_db_has_text_filed_list.json")
    if db in db_has_text_filed_list:
        from tool.client_es import ESClient

        es_client = ESClient()
        FLAG_GO = True
    else:
        FLAG_GO = False

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
            index=f"spider-{db}".lower(), queries=queries, size=50
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
    # python tool/spider_search.py

    # ---------- SearchColumn ----------
    SearchColumn = create_column_searcher(db="activity_1")
    results = SearchColumn(["female student", "male professor"])
    print(results)

    # ---------- SearchPath ----------
    # the column need to be selected is author.name, the columns need to be filtered are organization.name, domain.name, and publication.citation_num.
    start = "author.name"
    end = ["domain.name", "organization.name", "publication.citation_num"]
    path_finder = create_path_finder("academic")
    res = path_finder(start=start, end=end)
    print(res)

    # ---------- SearchValue ----------
    SearchValue = create_value_searcher("aan_1")
    res = SearchValue(["david", "roussel"])
    print(res)
