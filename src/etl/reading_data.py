from logging import getLogger

import polars as pl
from polars import dataframe

from src.etl import dataframe_utils
from src.utils import my_log

logger = getLogger("reading_data")
pl.Config.load_from_file("./config/polars.json")


def read_excel(path: str) -> pl.DataFrame:
    logger.info(f"Reading {path}...")

    df = pl.read_excel(path, engine="calamine")

    logger.info(f"Done reading {path}")
    return df


def read_enade_excel(path: str) -> pl.DataFrame:
    logger.info(f"Reading {path}...")

    table = dataframe_utils.enade_table()
    columns = list(table.columns.keys())

    df = pl.read_excel(path, engine="calamine", columns=columns)

    logger.info(f"Done reading {path}")
    return df


def read_multiple_parquet(globbing_pattern: str) -> pl.DataFrame:
    logger.info(f"Reading {globbing_pattern}...")

    df = pl.read_parquet(globbing_pattern)

    logger.info(f"Done reading {globbing_pattern}")

    return df
