def init_actions_spider(db, _hint=True):
    from tool.spider_execution import create_execute_sql
    from tool.spider_search import (
        create_column_searcher,
        create_path_finder,
        create_value_searcher,
    )

    SearchColumn = create_column_searcher(db=db)
    SearchValue = create_value_searcher(db=db)
    FindShortestPath = create_path_finder(db=db)
    ExecuteSQL = create_execute_sql(db=db, _hint=_hint)
    return SearchColumn, SearchValue, FindShortestPath, ExecuteSQL


def init_actions_bird(db, _hint=True):
    from tool.bird_execution import create_execute_sql
    from tool.bird_search import (
        create_column_searcher,
        create_path_finder,
        create_value_searcher,
    )

    SearchColumn = create_column_searcher(db=db)
    SearchValue = create_value_searcher(db=db)
    FindShortestPath = create_path_finder(db=db)
    ExecuteSQL = create_execute_sql(db=db, _hint=_hint)
    return SearchColumn, SearchValue, FindShortestPath, ExecuteSQL
