import logging

import duckdb
import polars as pl

logger = logging.getLogger("duckdb_sql")


def read_query_content(query_path: str) -> str:
    query = ""
    with open(query_path, "r") as file:
        query = file.read()

    return query


def read_query(query_path: str) -> pl.DataFrame:
    query_content = read_query_content(query_path)

    pl_df = duckdb.sql(query_content).pl()

    return pl_df
