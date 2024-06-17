import logging

from weasyprint import HTML

from logs import my_log
from src.presentation import images, jinja_template, polars_to_html
from src.utils import duckdb_sql

logger = logging.getLogger("pdf")
logging.getLogger("fontTools").setLevel(logging.WARNING)
logging.getLogger("weasyprint").setLevel(logging.WARNING)


def create_pdf(html_string: str, path_to_save_pdf: str) -> None:
    logger.info("Creating PDF...\n")

    HTML(string=html_string).write_pdf(path_to_save_pdf)

    logger.info("Done!\n")


def create_enade_report(
    html_path: str,
    css_path: str,
    cover_image_path: str,
    logo_image_path: str,
    query_file_path: str,
    path_to_save_pdf: str,
) -> None:
    df = duckdb_sql.read_query(query_file_path)

    table = polars_to_html.transform_df_in_tablehtml(df)

    cover_image = images.embedded_image(cover_image_path)
    logo_image = images.embedded_image(logo_image_path)

    html_context = {
        "cover_image": cover_image,
        "cols": table.cols_name,
        "table": table.table,
    }

    css_context = {
        "logo_image": logo_image,
    }

    html_string = jinja_template.render_final_html_special(
        html_path, html_context, css_path, css_context
    )

    create_pdf(html_string, path_to_save_pdf)
