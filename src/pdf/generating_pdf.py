import base64
import logging
from pathlib import Path

import duckdb
import polars as pl
from jinja2 import Template
from weasyprint import HTML

from logs import my_log
from src.utils import read_sql_query

logger = logging.getLogger("generating_pdf")
pl.Config.load_from_file("./config/polars.json")
logging.getLogger("fontTools").setLevel(logging.WARNING)
logging.getLogger("weasyprint").setLevel(logging.WARNING)


def get_content_file(path_file: str) -> str:
    """This function returns the content of a file

    Args:
        path_file (str): relative path

    Returns:
        str: file content
    """

    path = Path(path_file).resolve()
    with open(path, "r") as file:
        content = file.read()

    return content


def get_template(filename: str) -> Template:
    """This function returns a file as a jinja2 template

    Args:
        filename (str): filename

    Returns:
        Template: jinja2 template object
    """

    content = get_content_file(f"resources/templates/{filename}")
    template = Template(content)
    return template


def get_styles(style_filename: str) -> str:
    content = get_content_file(f"resources/styles/{style_filename}")
    return content


@my_log.debug_log(logger)
def render_template(
    template_filename: str,
    style_filename: str,
    context: dict,
) -> str:
    template = get_template(template_filename)

    style = get_styles(style_filename)

    context["styles"] = style

    html_body = template.render(context)

    return html_body


def creating_pdf(
    template_filename: str,
    style_filename: str,
    query_file_path: str,
    pdf_file_path: str,
) -> None:
    # 1) query
    query = read_sql_query.read_query(query_file_path)

    # 2) Gerar resultado query
    data: pl.DataFrame = duckdb.sql(query).pl()

    data_dict = data.to_dict(as_series=False)

    # 3) Colunas e linhas
    cols_name = list(data_dict.keys())
    cols_values = list(data_dict.values())

    len_row = len(cols_values[0])

    table = []
    for i in range(len_row):
        table.append([])
        for j in range(len(cols_values)):
            table[i].append(cols_values[j][i])

    # 4) Gerar imagem
    data_uri = base64.b64encode(
        open("./resources/images/enade.jpg", "rb").read()
    ).decode("utf-8")

    embedded_image = f"data:image/png;base64,{data_uri}"

    context = {
        "embedded_image": embedded_image,
        "alt_image": "ENADE LOGO",
        "cols": cols_name,
        "table": table,
    }

    # 5) Gerar HTML
    html = render_template(template_filename, style_filename, context)

    # 6) Gerar PDF
    HTML(string=html).write_pdf(pdf_file_path)

    logger.info("PDF created successfully.\n")
