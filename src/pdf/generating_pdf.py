import logging
from pathlib import Path

import pdfkit
from jinja2 import Template

from logs import my_log

logger = logging.getLogger("generating_pdf")


@my_log.debug_log(logger)
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


@my_log.debug_log(logger)
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


def creating_pdf(
    template_html_path: str, query_file_path: str, pdf_file_path: str
) -> None:
    # Convert HTML to PDF
    pdfkit.from_file(template_html_path, pdf_file_path)

    print("PDF created successfully.")
