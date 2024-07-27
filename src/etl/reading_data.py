from logging import getLogger

import polars as pl

from src.etl import dataframe_utils
from src.utils import my_log

logger = getLogger("reading_data")
pl.Config.load_from_file("./config/polars.json")


def read_enade_excel(path: str) -> pl.DataFrame:
    """
    Leia um arquivo Excel do caminho especificado, selecionando apenas as colunas
    especificadas na tabela ENADE.

    Args:
        path: O caminho para o arquivo Excel.

    Returns:
        O DataFrame contendo os dados do arquivo Excel, com apenas as colunas especificadas na tabela ENADE.
    """

    logger.info(f"Reading {path}...")

    # Defina as colunas a serem selecionadas do arquivo Excel
    tabela_auxiliar = dataframe_utils.TabelaAuxiliarEnade()

    colunas = tabela_auxiliar.lista_nomes_colunas_originais()

    # Leia o arquivo Excel, selecionando apenas as colunas especificadas
    df = pl.read_excel(path, engine="calamine", columns=colunas)

    logger.info(f"Done reading {path}")

    return df


def read_multiple_parquet(globbing_pattern: str) -> pl.DataFrame:
    """
    Lê vários arquivos Parquet a partir do padrão de glob fornecido.

    Args:
        globbing_pattern: O padrão de glob para corresponder aos arquivos
                                Parquet.

    Returns:
        O DataFrame contendo os dados de todos os arquivos Parquet correspondentes ao padrão de glob.
    """

    logger.info(f"Reading {globbing_pattern}...")

    # Ler os arquivos Parquet e concatená-los em um único DataFrame
    df = pl.read_parquet(globbing_pattern)

    logger.info(f"Done reading {globbing_pattern}")

    return df
