import logging

import duckdb
import polars as pl

from src.utils import my_log

logger = logging.getLogger("duckdb_sql")


@my_log.debug_log(logger)
def read_query_content(query_path: str) -> str:
    query = ""
    with open(query_path, "r") as file:
        query = file.read()

    return query


@my_log.debug_log(logger)
def read_query(query_path: str) -> pl.DataFrame:
    query_content = read_query_content(query_path)

    pl_df = duckdb.sql(query_content).pl()

    return pl_df
