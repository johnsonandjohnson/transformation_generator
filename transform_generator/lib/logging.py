import logging
import logging.config
from os import path
from pathlib import Path


def get_logger(module_name: str, logger_name: str = 'tgLogger', logger_config_path: str = 'logging.conf'):
    if path.isfile(logger_config_path):
        logging.config.fileConfig(logger_config_path)
    else:
        logging.config.fileConfig(path.join(Path(__file__).parent.parent.parent, logger_config_path))

    logger = logging.getLogger(f'{logger_name}')

    return logger
