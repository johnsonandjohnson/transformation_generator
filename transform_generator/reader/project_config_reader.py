import csv
from typing import Dict, List

from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.logging import get_logger

logger = get_logger(__name__)


def read_project_config_csvs(filenames: List[str]) -> Dict[str, ProjectConfigEntry]:
    """
    This function takes in a filepath/name to the generator configuration file and reads in the csv.
    @param filenames: List of strings of the paths to the generator configuration files.
    @return: A dictionary with the configuration file name as the key and the generator config entry as the value.
    """
    project_config = {}
    logger.info('Loading project config files')
    for filename in filenames:
        config_from_csv = {}
        logger.info('Loading project config file: %s', filename)
        with open(filename, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                config_from_csv[row['config_filename']] = ProjectConfigEntry(row['config_filename'],
                                                                             row['group_name'],
                                                                             row['schedule_frequency'],
                                                                             row['schedule_days'],
                                                                             row['parallel_db_name'],
                                                                             row['base_path'],
                                                                             row['notebooks_to_execute'],
                                                                             row['cluster_name'],
                                                                             row.get('ddl_wrapper_script', ''),
                                                                             row.get('sequence', None))
        if not config_from_csv:
            raise ValueError('Empty generator config file : ' + filename)
        project_config = {
            **project_config,
            **config_from_csv
        }
    return project_config
