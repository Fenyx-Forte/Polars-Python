from logging import getLogger

import polars as pl

from app.utils import my_log

logger = getLogger("loading_data")
pl.Config.load_from_file("./config/polars.json")


def write_parquet(df: pl.DataFrame, path: str) -> None:
    """Escreve um DataFrame em formato Parquet em um determinado caminho.

    Args:
        df: O DataFrame a ser escrito.
        path: O caminho onde o DataFrame será salvo.

    Returns:
        Nada.
    """
    logger.info(f"Writing {path}...")
    df.write_parquet(path)
    logger.info("Done!\n")


def write_csv(df: pl.DataFrame, path: str) -> None:
    """Escreve um DataFrame em formato CSV em um determinado caminho.

    Args:
        df: O DataFrame a ser escrito.
        path: O caminho onde o DataFrame será salvo.

    Returns:
        Nada.
    """
    logger.info(f"Writing {path}")
    df.write_csv(path)
    logger.info("Done!\n")
