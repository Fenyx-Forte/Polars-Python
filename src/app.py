from logging import getLogger

from logs import my_log
from src.etl import etl_routine
from src.presentation import pdf

logger = getLogger("app")


def main() -> None:
    logger.info("Starting ETL routine...\n")

    path_folder_to_find = "data/raw"

    path_folder_to_save = "data/processed"

    etl_routine.routine_all_enade(path_folder_to_find, path_folder_to_save)

    globbing_pattern = "data/processed/enade_20*.parquet"

    etl_routine.concatenate_enade(globbing_pattern)

    logger.info("ETL routine finished\n")


def create_enade_report() -> None:
    logger.info("Starting PDF creation...\n")

    html_path = "resources/templates/enade.html"
    css_path = "resources/styles/enade.css"
    cover_image_path = "resources/images/cover_enade.jpg"
    logo_image_path = "resources/images/logo_enade.png"
    query_file_path = "sql/query/enade_2021.sql"
    path_to_save_pdf = "reports/enade_2021.pdf"

    pdf.create_enade_report(
        html_path,
        css_path,
        cover_image_path,
        logo_image_path,
        query_file_path,
        path_to_save_pdf,
    )

    logger.info("PDF creation finished")
