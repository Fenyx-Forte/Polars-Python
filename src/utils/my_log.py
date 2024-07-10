import logging.config
from functools import wraps
from logging import Logger

logging.config.fileConfig(
    fname="config/log_config.ini", disable_existing_loggers=False
)
