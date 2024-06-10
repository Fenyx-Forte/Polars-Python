from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("loading_data")
pl.Config.load_from_file("./config/polars.json")


@my_log.debug_log(logger)
def write_parquet(df: pl.DataFrame, path: str) -> None:
    logger.info(f"Writing {path}...")
    df.write_parquet(path)
    logger.info("Done!\n")


@my_log.debug_log(logger)
def write_csv(df: pl.DataFrame, path: str) -> None:
    logger.info(f"Writing {path}")
    df.write_csv(path)
    logger.info("Done!\n")
