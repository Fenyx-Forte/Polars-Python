from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("reading_data")
pl.Config.load_from_file("./config/polars.json")


@my_log.debug_log(logger)
def read_excel(path: str) -> pl.DataFrame:
    logger.info(f"Reading {path}...")

    df = pl.read_excel(path, engine="calamine")

    logger.info(f"Done reading {path}")
    return df


@my_log.debug_log(logger)
def read_multiple_parquet(globbing_pattern: str) -> pl.DataFrame:
    logger.info(f"Reading {globbing_pattern}...")

    df = pl.read_parquet(globbing_pattern)

    logger.info(f"Done reading {globbing_pattern}")

    return df
