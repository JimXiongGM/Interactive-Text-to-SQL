import re
import sqlite3

import chromadb
import sqlparse

chroma_client = None


def init_chroma_client(name):
    global chroma_client
    if chroma_client is None:
        chroma_client = chromadb.PersistentClient(path=f"./database/db_chroma_{name}")
    return chroma_client


def get_tables(cursor: sqlite3.Cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    tables = [table[0] for table in tables if table[0] != "sqlite_sequence"]
    return tables


def get_columns_types(cursor: sqlite3.Cursor, table: str):
    cursor.execute(f"PRAGMA table_info(`{table}`)")
    # [(cid, name, type, notnull, dflt_value, pk)]
    columns = cursor.fetchall()
    columns_types = [(col[1], col[2]) for col in columns]
    return columns_types


def get_primary_key(cursor: sqlite3.Cursor, table_name):
    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = cursor.fetchall()
    primary_keys = [col[1] for col in columns if col[5] == 1]
    return primary_keys


def get_foreign_keys(cursor: sqlite3.Cursor, table_name):
    cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)")
    foreign_keys = cursor.fetchall()
    return foreign_keys


"""
All:
{'decimal', 'real', 'integer', 'double', 'boolean', 'bool', 'varchar2', 'text', 'datetime', 'smallint', 'bit', 'blob', 'char', 'tinyint unsigned', 'int', 'mediumint unsigned', 'number', 'date', 'year', 'varchar', 'character varchar', 'timestamp', 'smallint unsigned', 'numeric', 'float', 'bigint'}
"""
# spider
TEXT_FILED_KW = [
    "varchar",
    "text",
    "char",
]


def is_text_filed(col_type):
    col_type = col_type.lower()
    for _key in TEXT_FILED_KW:
        if _key in col_type:
            return True
    return False


# -------------- for spider -------------- #


def extract_where_clause(sql: str):
    # Parse the SQL query
    parsed_query = sqlparse.parse(sql)
    if not parsed_query:
        return "No valid SQL query found."

    # Find the WHERE clause
    where_clause = None
    for token in parsed_query[0].tokens:
        if isinstance(token, sqlparse.sql.Where):
            where_clause = token
            break

    # Combine the WHERE clause tokens into a single string
    if where_clause:
        where_clause_str = "".join(
            token.value
            for token in where_clause.tokens
            if token.ttype is not sqlparse.tokens.Token.Whitespace
        )
        # Remove the initial "WHERE" keyword
        where_clause_str = where_clause_str[5:].strip()
        if "select " not in where_clause_str.lower():
            return where_clause_str
        else:
            return "No WHERE clause found in the SQL query."
    else:
        return "No WHERE clause found in the SQL query."


def extract_table_alias(sql):
    alias_pattern = re.compile(
        r"(?:\b(?:from|join)\s+[\w\d_]+\s+)(?:as\s+)?\b[\w\d_]+\b"
    )
    matches = alias_pattern.findall(sql.lower())

    details = [
        m.replace("from", "")
        .replace("join", "")
        .replace(" where", "")
        .replace(" group", "")
        .replace(" on", "")
        .replace(" order", "")
        .replace(" except", "")
        .replace(" union", "")
        .strip()
        for m in matches
    ]
    details = [m for m in details if m.count(" ") > 0]

    return details


def has_as_in_select_clause(sql):
    match = re.search(r"select (.+?) from", sql, re.IGNORECASE)
    if match:
        select_clause = match.group(1)
        if re.search(r"\sas\s", select_clause, re.IGNORECASE):
            return True
        else:
            return False
    else:
        return True


def contain_in_clause(sql_query):
    """
    Checks if the given SQL query contains an IN clause.
    """
    # Regular expression to match the IN clause in the WHERE condition
    pattern = re.compile(r"WHERE\s+\w+\s+IN\s*\(.*?\)", re.IGNORECASE)
    return bool(pattern.search(sql_query))


def rewrite_sql_rm_cast(query):
    """
    Spider eval does not support the CAST syntax; rewrite CAST(expression AS type) as expression.
    """
    pattern = re.compile(r"CAST\((.*?) AS [^\)]+\)", re.IGNORECASE)
    rewritten_query = pattern.sub(lambda match: match.group(1), query)

    return rewritten_query


def contain_op_in_select_clause(sql_query):
    # Regular expression to match the COUNT function in the SELECT clause
    pattern = re.compile(
        r"SELECT\s+((?:(?!FROM).)+)\s+(COUNT|SUM|MAX|MIN|AVG)\(\s*[^)]+\s*\)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql_query)

    if not match:
        return False

    # Ensure COUNT is within the SELECT clause and not part of any subquery or other clause
    select_clause = match.group(1)
    return "FROM" not in select_clause.upper().split()


def contain_multi_columns_in_select_clause(sql_query):
    if "SELECT * FROM" in sql_query:
        return True
    pattern = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE)
    res = pattern.search(sql_query)
    if res and res[0].count(", ") > 0:
        return True
    return False


def remove_column_aliases_in_select_clause(sql):
    """
    Trick. Remove column aliases in the SELECT clause.
    """
    pattern = re.compile(
        r"(\b(?:COUNT|SUM|AVG|MIN|MAX|etc)\(\w+\.\w+\))\s+\w+", re.IGNORECASE
    )
    modified_sql = pattern.sub(r"\1", sql)
    return modified_sql


def has_multiple_group_by_columns(sql_query: str) -> bool:
    group_by_pattern = re.compile(
        r"\bGROUP\s+BY\s+(.+?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*$)", re.IGNORECASE
    )
    match = group_by_pattern.search(sql_query)

    if match:
        group_by_clause = match.group(1)
        columns = [col.strip() for col in group_by_clause.split(",")]
        return len(columns) > 1

    return False


def extract_last_valid_sql(dialog):
    """
    find last valid sql, except the one start with 'SELECT * FROM'
    """
    default_sql = ""
    for dia in reversed(dialog):
        if dia["role"] == "assistant" and "ExecuteSQL(" in dia["content"]:
            # extract sql from ExecuteSQL("...")
            pred_sql = dia["content"].split("ExecuteSQL(")[1][:-1]
            try:
                pred_sql = eval(pred_sql)
            except:
                continue
            if not pred_sql.startswith("SELECT * FROM"):
                return pred_sql
            if not default_sql:
                default_sql = pred_sql
    return default_sql or "None"


def is_dict(text):
    if "text filed." in text or "distinct count:" in text:
        return False
    if "min:" in text and "max:" in text:
        return False
    try:
        eval(text)
        return True
    except:
        return False


def jaccard_sim(golden, pred):
    for c in ["(", "'", ","]:
        golden = golden.replace(c, " ")
        pred = pred.replace(c, " ")
    golden = [i for i in golden.split() if i]
    pred = [i for i in pred.split() if i]
    intersection = len(set(golden) & set(pred))
    union = len(set(golden) | set(pred))
    return intersection / union


def round_floats_in_structure(data, precision=6):
    if isinstance(data, float):
        return round(data, precision)
    elif isinstance(data, list):
        return [round_floats_in_structure(item, precision) for item in data]
    elif isinstance(data, tuple):
        return tuple(round_floats_in_structure(item, precision) for item in data)
    else:
        return data


INVALID_RESULTS = set(
    [
        "[]",
        "[()]",
        "[(None,)]",
        "[(None, None)]",
        "[(0,)]",
        "[('',)]",
        "None",
        "[(0.0,)]",
    ]
)
