from logging import getLogger

import polars as pl

from src.etl import (
    dataframe_utils,
    loading_data,
    reading_data,
    transforming_data,
)
from src.utils import my_log

logger = getLogger("etl_routine")
pl.Config.load_from_file("./config/polars.json")


def get_raw_enade(path: str) -> pl.DataFrame:
    """
    Função para obter os dados brutos do arquivo Excel no caminho especificado.

    Args:
        path: O caminho para o arquivo Excel.

    Returns:
        O DataFrame contendo os dados brutos do arquivo Excel.
    """
    return reading_data.read_enade_excel(path)


def process_enade(raw_enade: pl.DataFrame) -> pl.DataFrame:
    """
    Processa os dados do arquivo Excel e retorna um DataFrame processado.

    Args:
        raw_enade: O DataFrame contendo os dados brutos do arquivo Excel.

    Returns:
        O DataFrame com as transformações aplicadas aos dados.
    """
    table = dataframe_utils.enade_table()

    transforming_data.verify_datatype(raw_enade, table)

    table = transforming_data.change_column_names(table)

    table = transforming_data.transform_columns(table)

    table = transforming_data.casting_columns(table)

    table = transforming_data.standardizing_data(table)

    table = transforming_data.shrinking_numerical_columns(table)

    return transforming_data.transform(raw_enade, table)


def save_enade(path_folder: str, filename: str, df: pl.DataFrame) -> None:
    """
    Salva um DataFrame em formato Parquet em um determinado caminho.

    Args:
        path_folder: O caminho onde o DataFrame será salvo.
        filename: O nome do arquivo de saída.
        df: O DataFrame a ser salvo.

    Returns:
        Nada.
    """
    loading_data.write_parquet(df, f"{path_folder}/{filename}.parquet")


def routine_enade(
    path: str,
    path_folder_to_save: str,
    filename: str,
) -> None:
    """
    Processa os dados do arquivo Excel e salva o resultado em formato Parquet.

    Args:
        path: O caminho do arquivo Excel a ser processado.
        path_folder_to_save: A pasta onde o DataFrame processado será salvo.
        filename: O nome do arquivo que será criado.

    Returns:
        Nada.
    """
    raw_enade = get_raw_enade(path)

    processed_enade = process_enade(raw_enade)

    save_enade(path_folder_to_save, filename, processed_enade)


def routine_all_enade(
    path_folder_to_find: str, path_folder_to_save: str
) -> None:
    """
    Realiza a rotina de ETL para todos os arquivos Excel de conceito ENADE.

    Args:
        path_folder_to_find: O caminho onde os arquivos Excel serão procurados.
        path_folder_to_save: O caminho onde os DataFrames processados serão salvos.

    Returns:
        Nada.
    """
    logger.info("Iniciando a rotina ETL...\n")

    years = [2021, 2019, 2018, 2017]
    for year in years:
        path = f"{path_folder_to_find}/conceito_enade_{year}.xlsx"

        filename_to_save = f"enade_{year}"

        routine_enade(path, path_folder_to_save, filename_to_save)

    logger.info("Rotina ETL finalizada")


def concatenate_enade(globbing_pattern: str) -> None:
    """
    Concatena vários arquivos Parquet em um único arquivo Parquet.

    Args:
        globbing_pattern: O padrão de glob para corresponder aos arquivos Parquet.

    Returns:
        Nada.
    """
    all_enade_files = reading_data.read_multiple_parquet(globbing_pattern)
    loading_data.write_parquet(all_enade_files, "data/processed/enade.parquet")
