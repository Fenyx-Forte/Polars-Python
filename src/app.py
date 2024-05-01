from logging import getLogger

from logs import my_log
from src.etl import etl_routine
from src.pdf import generating_pdf

logger = getLogger("app")


def main():
    logger.info("Starting ETL routine")

    path_folder_to_find = "data/raw"

    path_folder_to_save = "data/processed"

    etl_routine.routine_all_enade(path_folder_to_find, path_folder_to_save)

    globbing_pattern = "data/processed/enade_20*.parquet"

    etl_routine.concatenate_enade(globbing_pattern)

    logger.info("ETL routine finished")


def creating_pdf():
    logger.info("Starting PDF creation")

    template_html_path = "resources/template/template1.html"
    query_file_path = ""
    pdf_file_path = "resources/pdf/output.pdf"

    generating_pdf.creating_pdf(
        template_html_path, query_file_path, pdf_file_path
    )

    logger.info("PDF creation finished")
