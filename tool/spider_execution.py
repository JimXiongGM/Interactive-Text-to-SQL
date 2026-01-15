import sqlite3

from tool.utils import (
    contain_in_clause,
    contain_multi_columns_in_select_clause,
    contain_op_in_select_clause,
    extract_table_alias,
    extract_where_clause,
)
from utils import timeout


def create_execute_sql(db, _hint=True):
    db_file = f"dataset/spider/test_database/{db}/{db}.sqlite"
    FLAG = True
    FLAG_table_alias = True

    def set_flag():
        nonlocal FLAG
        FLAG = False

    def get_flag():
        nonlocal FLAG
        return FLAG

    @timeout(100)
    def execute_sql(sql, str_mode=True):
        sql = sql.strip()
        if sql and sql[0] == sql[-1] == '"':
            sql = sql.strip('"')

        _sql = sql.lower()
        if "(select" in sql:
            _sql = sql[: sql.index("(select", 0)]

        # rule: left join is not allowed
        if _hint and str_mode and " left join " in _sql:
            return "Error. Do not support 'left join'."

        # rule: Do not support 'cast' syntax
        # if "cast(" in _sql:
        #     return "Error. Do not support 'cast' syntax."

        # rule: Do not support 'as' in select clause
        # if _hint and "cast(" not in _sql and has_as_in_select_clause(_sql):
        #     return "Error. Do not support 'as' in select clause."

        # rule: Do not support table alias
        nonlocal FLAG_table_alias
        if _hint and str_mode and FLAG_table_alias and extract_table_alias(_sql):
            FLAG_table_alias = False
            return f"Error. Do not use table alias, please write the full table name."

        # rule: Do not support 'in' syntax
        if _hint and str_mode and contain_in_clause(_sql):
            return "Error. Do not support 'where ... in (...)' syntax, please use 'OR' instead."

        # trick: Do not support '_id = ' in where clause
        where_clause = extract_where_clause(_sql)
        if _hint and str_mode and "_id = " in where_clause:
            return "Error. Do not use '_id = ' in where clause, please write the full condition."

        connection = sqlite3.connect(db_file)
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()

        nonlocal FLAG
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            result = sorted(set(result))
            if str_mode:
                result = str(result)
                if len(result) > 200:
                    result = result[:200] + "..."
                # trick: when return multi columns, make sure the order is correct.
                if (
                    _hint
                    and result != "[]"
                    and FLAG
                    and (
                        contain_multi_columns_in_select_clause(_sql)
                        or contain_op_in_select_clause(_sql)
                    )
                ):
                    result += " (Hint: DOUBLE-CHECK the columns in the SELECT clause, do not select irrelevant columns, and the order of the columns must strictly match the requirements of the question. Re-call ExecuteSQL function if necessary.)"
            cursor.close()
            return result
        except Exception as e:
            cursor.close()
            return str(e)

    execute_sql.set_flag = set_flag
    execute_sql.get_flag = get_flag

    return execute_sql


if __name__ == "__main__":
    ExecuteSQL = create_execute_sql(db="network_1")
    r = ExecuteSQL(
        "SELECT ID FROM Highschooler EXCEPT (SELECT student_id FROM Friend UNION SELECT friend_id FROM Friend)"
    )
    print(r)
