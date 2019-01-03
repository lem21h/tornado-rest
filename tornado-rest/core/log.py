# coding=utf-8
import logging.handlers
from logging import Formatter
from os import path
from typing import Union, Tuple, List

LOG_TORNADO_ACCESS = 'tornado.access'
LOG_TORNADO_APPLICATION = 'tornado.application'
LOG_TORNADO_GENERAL = 'tornado.general'

LOG_EXCEPTIONS = 'app.exception'
LOG_MAIN = 'app.main'
LOG_EMAIL = 'app.email'

_DEFAULT_LOGS = (LOG_TORNADO_ACCESS, LOG_TORNADO_APPLICATION, LOG_TORNADO_GENERAL, LOG_MAIN, LOG_EXCEPTIONS)
_ALL_LOGS = _DEFAULT_LOGS + (LOG_EMAIL,)


def defineLogging(log_config, logs_to_define: Union[List, Tuple, str] = None):
    formatter = logging.Formatter(log_config.logformat)
    to_file = log_config.output_file is True
    to_console = log_config.output_console is True

    if not logs_to_define:
        logs_to_define = _DEFAULT_LOGS

    if logs_to_define:
        if isinstance(logs_to_define, (list, set, tuple)):
            for log_name in logs_to_define:
                _init_single_log(formatter, log_config.level, log_name, log_config.relpath, to_console, to_file)
        else:
            _init_single_log(formatter, log_config.level, logs_to_define, log_config.relpath, to_console, to_file)

    _init_single_log(formatter, logging.DEBUG, LOG_EXCEPTIONS, log_config.relpath, True, True)


def _init_single_log(formatter: Formatter, log_level: int, log_name: str, log_path: str, to_console: bool, to_file: bool):
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    if to_file:
        lh = logging.handlers.RotatingFileHandler(path.join(log_path, '{}.log'.format(log_name)), maxBytes=10000000, backupCount=5)
        lh.setFormatter(formatter)
        logger.addHandler(lh)
    if to_console:
        lh = logging.StreamHandler()
        lh.setFormatter(formatter)
        logger.addHandler(lh)
