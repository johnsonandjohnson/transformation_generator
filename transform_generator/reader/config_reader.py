import csv
from os.path import split

from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.logging import get_logger

logger = get_logger(__name__)


def read_config_csv(file_path):
    """
    Reads the CSV table config file located at file_path and returns a list of ConfigEntry objects.
    The config file must be accessible by the file system where this script is running.

    @param file_path: The path of the file to be read.
    @return: An array of one or more configuration mappings.
    @raise: FileNotFoundError - when CSV file path is not valid.
    @raise: ValueError - when CSV file is missing config data
    @raise: KeyError - when CSV file is missing an expected key.
    """
    logger.info("Loading config file: %s", file_path)

    _, filename = split(file_path)
    config_from_csv = []
    with open(file_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            load_synapse = row.get('load_synapse', 'N') == 'Y'
            config_from_csv.append(ConfigEntry(filename,
                                               row['target_table'],
                                               row['target_type'],
                                               row['target_language'],
                                               row['input_files'].split(','),
                                               row['dependencies'],
                                               row['tuning_parameter'].split(','),
                                               row.get('load_type', 'full'),
                                               load_synapse,
                                               row.get('sequence', None)))

    if not config_from_csv:
        raise ValueError('Empty Config File : ' + file_path)
    return config_from_csv
