from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("my_extraction")


@my_log.debug_log(logger)
def read_excel(path: str) -> pl.DataFrame:
    logger.info(f"Reading {path}")
    return pl.read_excel(path, engine="calamine")


def read_multiple_parquet(globbing_pattern: str) -> pl.DataFrame:
    logger.info(f"Reading {globbing_pattern}")

    return pl.read_parquet(globbing_pattern)
