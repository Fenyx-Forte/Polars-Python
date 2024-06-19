import logging
from pathlib import Path

from jinja2 import Template

from src.utils import my_log

logger = logging.getLogger("jinja_template")


@my_log.debug_log(logger)
def get_file_content(file_path: str) -> str:
    """This function returns the content of a file

    Args:
        file_path (str): relative path

    Returns:
        str: file content
    """

    path = Path(file_path).resolve()
    with open(path, "r") as file:
        content = file.read()

    return content


@my_log.debug_log(logger)
def get_template(file_path: str) -> Template:
    """This function returns a file as a jinja2 template

    Args:
        file_path (str): relative path

    Returns:
        Template: jinja2 template object
    """

    content = get_file_content(file_path)
    template = Template(content)
    return template


@my_log.debug_log(logger)
def get_css(file_path: str) -> str:
    content = get_file_content(file_path)

    return content


@my_log.debug_log(logger)
def render_template(
    template_path: str,
    context: dict,
) -> str:
    template = get_template(template_path)

    template_renderized = template.render(context)

    return template_renderized


@my_log.debug_log(logger)
def render_final_html_simple(
    html_path: str,
    html_context: dict,
    css_template: str,
) -> str:
    css_content = get_css(css_template)

    html_context["css"] = css_content

    html_renderized = render_template(html_path, html_context)

    return html_renderized


@my_log.debug_log(logger)
def render_final_html_special(
    html_path: str,
    html_context: dict,
    css_path: str,
    css_context: dict,
) -> str:
    css_renderized = render_template(css_path, css_context)

    html_context["css"] = css_renderized

    html_renderized = render_template(html_path, html_context)

    return html_renderized
