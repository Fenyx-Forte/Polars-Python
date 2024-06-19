import base64
import logging

from src.utils import my_log

logger = logging.getLogger("images")


@my_log.debug_log(logger)
def transform_image_in_base64(image_path: str) -> str:
    image_bytes = open(image_path, "rb").read()

    image_bytes_encode = base64.b64encode(image_bytes)

    image_string = image_bytes_encode.decode("utf-8")

    return image_string


@my_log.debug_log(logger)
def embedded_image(image_path: str) -> str:
    image_string = transform_image_in_base64(image_path)

    return f"data:image/png;base64,{image_string}"
