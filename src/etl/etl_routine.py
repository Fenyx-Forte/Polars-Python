from logging import getLogger

import polars as pl

from logs import my_log
from src.etl import reading_data, transforming_data, loading_data

logger = getLogger("etl_routine")
pl.Config.load_from_file("./config/polars.json")


@my_log.debug_log(logger)
def get_raw_enade(path: str) -> pl.DataFrame:
    return reading_data.read_excel(path)


@my_log.debug_log(logger)
def process_enade(raw_enade: pl.DataFrame) -> pl.DataFrame:
    table = transforming_data.enade_table()

    transforming_data.verify_columns(raw_enade, table)

    table = transforming_data.change_column_names(table)

    table = transforming_data.transform_columns(table)

    table = transforming_data.casting_columns(table)

    table = transforming_data.standardizing_data(table)

    table = transforming_data.shrinking_numerical_columns(table)

    return transforming_data.transform(raw_enade, table)


@my_log.debug_log(logger)
def save_enade(path_folder: str, filename: str, df: pl.DataFrame) -> None:
    loading_data.write_parquet(df, f"{path_folder}/{filename}.parquet")


@my_log.debug_log(logger)
def routine_enade(path: str, path_folder_to_save: str, filename: str) -> None:
    raw_enade = get_raw_enade(path)

    processed_enade = process_enade(raw_enade)

    save_enade(path_folder_to_save, filename, processed_enade)


@my_log.debug_log(logger)
def routine_all_enade(
    path_folder_to_find: str, path_folder_to_save: str
) -> None:
    logger.info("Starting ETL routine...\n")

    years = [2021, 2019, 2018, 2017]
    for year in years:
        path = f"{path_folder_to_find}/conceito_enade_{year}.xlsx"

        filename_to_save = f"enade_{year}"

        routine_enade(path, path_folder_to_save, filename_to_save)

    logger.info("ETL routine finished")


def concatenate_enade(globbing_pattern: str) -> None:
    all_enade_files = reading_data.read_multiple_parquet(globbing_pattern)
    loading_data.write_parquet(all_enade_files, "data/processed/enade.parquet")
