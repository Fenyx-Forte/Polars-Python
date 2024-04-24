from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("my_load")


@my_log.debug_log(logger)
def write_parquet(df: pl.DataFrame, path: str) -> None:
    logger.info(f"Writing {path}")
    df.write_parquet(path)


@my_log.debug_log(logger)
def write_csv(df: pl.DataFrame, path: str) -> None:
    logger.info(f"Writing {path}")
    df.write_csv(path)
