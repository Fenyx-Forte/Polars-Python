import logging.config
from functools import wraps
from logging import Logger

logging.config.fileConfig(
    fname="config/log_config.ini", disable_existing_loggers=False
)


def debug_log(logger: Logger):
    """
    This function is a decorator that logs the entrance and exit of a function.

    Args:
        logger (Logger): The logger of the module where the function is.
    """

    def decorator_debug_log(func):
        @wraps(
            func
        )  # This is to preserve the metadata of the original function
        def wrapper(*args, **kwargs):
            logger.debug(f"Entering {func.__name__}")

            result = func(*args, **kwargs)

            logger.debug(f"Exiting {func.__name__}")

            return result

        return wrapper

    return decorator_debug_log
