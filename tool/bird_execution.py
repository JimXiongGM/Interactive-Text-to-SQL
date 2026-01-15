import os
import sqlite3

from tool.utils import (
    contain_multi_columns_in_select_clause,
    contain_op_in_select_clause,
)
from utils import timeout


def create_execute_sql(db, _hint=True):
    db_file = f"dataset/bird/dev_databases/{db}/{db}.sqlite"
    if not os.path.exists(db_file):
        db_file = f"dataset/bird/train_databases/{db}/{db}.sqlite"
        if not os.path.exists(db_file):
            raise FileNotFoundError(f"Database file not found: {db_file}")
    FLAG = True

    def set_flag():
        nonlocal FLAG
        FLAG = False

    def get_flag():
        nonlocal FLAG
        return FLAG

    @timeout(120)
    def execute_sql(sql, str_mode=True):
        sql = sql.strip()
        if sql and sql[0] == sql[-1] == '"':
            sql = sql.strip('"')
        _sql = sql.lower()

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        nonlocal FLAG
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            result = sorted(set(result))
            if str_mode:
                result = str(result)
                if len(result) > 200:
                    result = result[:200] + "..."

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
    # python tool/bird_execution.py
    db = "codebase_community"
    ExecuteSQL = create_execute_sql(db)
    r = ExecuteSQL(
        "SELECT Id, DisplayName FROM users WHERE Id IN (2, 3, 330, 2570, 7837, 8378, 10135, 10594, 12170, 13304, 37433, 39990, 44568, 44939, 45877)"
    )
    print(r)
