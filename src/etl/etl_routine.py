from logging import getLogger

import polars as pl

from logs import my_log
from src.etl import my_extraction, my_load, my_transform

logger = getLogger("etl_routine")


@my_log.debug_log(logger)
def get_raw_enade(path: str) -> pl.DataFrame:
    return my_extraction.read_excel(path)


@my_log.debug_log(logger)
def process_enade(raw_enade: pl.DataFrame) -> pl.DataFrame:
    renamed_columns = {
        "Ano": ["ano", pl.Int16],
        "Área de Avaliação": ["area_avaliacao", pl.Categorical],
        "Nome da IES": ["ies", pl.Categorical],
        "Organização Acadêmica": ["org_acad", pl.Categorical],
        "Categoria Administrativa": ["cat_acad", pl.Categorical],
        "Modalidade de Ensino": ["mod_ens", pl.Categorical],
        "Município do Curso": ["municipio_curso", pl.Categorical],
        "Sigla da UF": ["sigla_uf", pl.Categorical],
        "Nº de Concluintes Inscritos": ["num_conc_insc", pl.Int16],
        "Nº  de Concluintes Participantes": ["num_conc_part", pl.Int16],
        "Nota Bruta - FG": ["nota_bruta_fg", pl.Float32],
        "Nota Padronizada - FG": ["nota_padronizada_fg", pl.Float32],
        "Nota Bruta - CE": ["nota_bruta_ce", pl.Float32],
        "Nota Padronizada - CE": ["nota_padronizada_ce", pl.Float32],
        "Conceito Enade (Contínuo)": ["conc_enade_cont", pl.Float32],
    }

    transformations = my_transform.expr_cast_columns(raw_enade, renamed_columns)

    return my_transform.transform(raw_enade, transformations)


@my_log.debug_log(logger)
def save_enade(path_folder: str, filename: str, df: pl.DataFrame) -> None:
    my_load.write_parquet(df, f"{path_folder}/{filename}.parquet")


@my_log.debug_log(logger)
def routine_enade(path: str, path_folder_to_save: str, filename: str) -> None:
    raw_enade = get_raw_enade(path)

    processed_enade = process_enade(raw_enade)

    save_enade(path_folder_to_save, filename, processed_enade)


@my_log.debug_log(logger)
def routine_all_enade(
    path_folder_to_find: str, path_folder_to_save: str
) -> None:
    logger.info("Starting ETL routine")

    years = [2021, 2019, 2018, 2017]
    for year in years:
        path = f"{path_folder_to_find}/conceito_enade_{year}.xlsx"

        filename_to_save = f"enade_{year}"

        routine_enade(path, path_folder_to_save, filename_to_save)

    logger.info("ETL routine finished")


def concatenate_enade(globbing_pattern: str) -> None:
    all_enade_files = my_extraction.read_multiple_parquet(globbing_pattern)
    my_load.write_parquet(all_enade_files, "data/processed/enade.parquet")
