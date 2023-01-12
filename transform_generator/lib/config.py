from os import listdir
from os.path import isfile, join, exists, isdir

from .config_entry import ConfigEntry
from .data_mapping import DataMapping
from .project_config_entry import ProjectConfigEntry
from .logging import get_logger

from typing import List, Dict, Tuple

from .mapping import load_mappings
from ..reader.config_reader import read_config_csv
from ..reader.project_config_reader import read_project_config_csvs

logger = get_logger(__name__)


def get_config_by_mapping_filename(configs_by_filename: Dict[str, List[ConfigEntry]]) -> Dict[str, ConfigEntry]:
    config_by_mapping_filename = {}
    for filename, entries in configs_by_filename.items():
        for entry in entries:
            if entry.target_type != 'program':
                for mapping_filename in entry.input_files:
                    config_by_mapping_filename[mapping_filename] = entry
    return config_by_mapping_filename


def get_target_table_partition_key(config: ConfigEntry):
    return config.target_table


def get_config_by_target_table(configs_by_filename: Dict[str, List[ConfigEntry]]) -> Dict[str, ConfigEntry]:
    config_by_target_table = {}
    for filename, entries in configs_by_filename.items():
        for entry in entries:
            key = get_target_table_partition_key(entry)
            config_by_target_table[key] = entry
    return config_by_target_table


def get_config_filename_by_target_table(configs_by_filename: Dict[str, List[ConfigEntry]]) -> Dict[str, str]:
    config_by_target_table = {}
    for filename, entries in configs_by_filename.items():
        for entry in entries:
            key = get_target_table_partition_key(entry)
            config_by_target_table[key] = filename
    return config_by_target_table


def load_config_directory(directory: str) -> Dict[str, List[ConfigEntry]]:
    """
    Load all Transformation Generator in the given directory and return a dictionary of  config file name to entries
    :param directory: The path to the directory containing the config files.
    :return: A dictionary with the config file name as a key and a list of ConfigEntry objects as values.
    """
    if not (exists(directory) or isdir(directory)):
        raise FileNotFoundError("Directory {} not found. ".format(directory))

    config_file_to_entries = {}
    total_count = 0
    for file_name in listdir(directory):
        full_path = join(directory, file_name)
        if isfile(full_path):
            entries = read_config_csv(full_path)
            config_file_to_entries[file_name] = entries
            total_count += len(entries)

    logger.info("%d config entries loaded from %d config files", total_count,
                len(config_file_to_entries.keys()))
    return config_file_to_entries


def get_platform_for_configs(configs: List[ConfigEntry]) -> str:
    """
    Scans a sequence of ConfigEntries to determine if it is for the Azure or Cloudera platform.
    If the target language is 'impala' or 'hive' for all entries, the platform will be 'edl.' If it is databricks for
    all entries, the platform will be 'azure.' Any other combination will raise an error.
    @param configs: A sequence of config entries
    @return: A string containing either 'azure' or 'cloudera'
    """
    language_set = set([str(entry.target_language).lower() for entry in configs])
    if language_set <= {'hive', 'impala'}:
        return 'cloudera'
    elif language_set == {'databricks'}:
        return 'databricks'
    else:
        raise Exception("Invalid set of target_languages found in config entries: " + str(language_set))


def get_config_structures(config_path: str, mapping_sheet_path: str, database_param_prefix: str,
                          project_config_paths: str, external_module_config_paths: str = '') -> Tuple[
    Dict[str, ConfigEntry], Dict[str, str], Dict[str, List[ConfigEntry]], bool, Dict[str, ProjectConfigEntry], Dict[
        str, DataMapping]]:
    """
    Creates the necessary data structures
    @param config_path: string path to config directory
    @param mapping_sheet_path: string path to base directory containing data mapping sheets
    @param database_param_prefix: String containing the prefix for the ADF Pipelines
    @param project_config_paths: A list of string path to the generator config files
    @param external_module_config_paths: A string of semicolon (;) delimited paths to external module config directories
    @return: A tuple containing the necessary data structures (config_by_mapping_filename,
        config_filename_by_target_table, configs_by_config_filename, database_variables, project_config,
        mappings_by_mapping_filename)
    """
    project_config_paths = project_config_paths.split(';')
    project_config = read_project_config_csvs(project_config_paths)

    configs_by_config_filename = load_config_directory(config_path)

    module_configs_by_config_filename = configs_by_config_filename.copy()
    if external_module_config_paths:
        external_module_config_paths = external_module_config_paths.split(';')
        for external_module_config_path in external_module_config_paths:
            module_configs_by_config_filename = {
                **module_configs_by_config_filename,
                **load_config_directory(external_module_config_path)
            }

    database_variables = False
    for project_config_entry in project_config.values():
        if project_config_entry.parallel_db_name != '':
            database_variables = True
            configs_by_config_filename_keys = list(configs_by_config_filename.keys())
            for key in configs_by_config_filename_keys:
                configs_by_config_filename[database_param_prefix + '_' + key] = configs_by_config_filename[key]
                del configs_by_config_filename[key]
            module_configs_by_config_filename_keys = list(module_configs_by_config_filename.keys())
            for key in module_configs_by_config_filename_keys:
                module_configs_by_config_filename[database_param_prefix + '_' + key] = module_configs_by_config_filename[key]
                del module_configs_by_config_filename[key]
            project_config_keys = list(project_config.keys())
            for key in project_config_keys:
                project_config[database_param_prefix + '_' + key] = project_config[key]
                del project_config[key]
            break
    config_by_mapping_filename = get_config_by_mapping_filename(configs_by_config_filename)
    config_filename_by_target_table = get_config_filename_by_target_table(module_configs_by_config_filename)

    mappings_by_mapping_filename = load_mappings(mapping_sheet_path, list(config_by_mapping_filename.keys()))
    for config_entry in config_by_mapping_filename.values():
        for input_file in config_entry.input_files:
            mappings_by_mapping_filename[input_file].config_entry = config_entry

    for filename, data_mapping in mappings_by_mapping_filename.items():
        data_mapping.config_entry = config_by_mapping_filename[filename]

    return (config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables,
            project_config, mappings_by_mapping_filename)


def get_new_config_structures(project_group, database_param_prefix: str, project_config_path: str,
                              external_module_config_paths: str = '') -> Tuple[
    Dict[str, ConfigEntry], Dict[str, str], Dict[str, List[ConfigEntry]], bool, Dict[str, ProjectConfigEntry], Dict[
        str, DataMapping]]:
    """
    Creates the necessary data structures
    @param project_group: A list of Project objects
    @param database_param_prefix: String containing the prefix for the ADF Pipelines
    @param project_config_path: A string path to the generator config file
    @param external_module_config_paths: A string of semicolon (;) delimited paths to external module config directories
    @return: A tuple containing the necessary data structures (config_filename_by_target_table,
    configs_by_config_filename, database_variables, project_config, mappings_by_mapping_filename)
    """
    config_by_mapping_filename, configs_by_config_filename, project_config, \
        mappings_by_mapping_filename = get_configs_mappings_from_data_mappings(project_group)

    module_configs_by_config_filename = configs_by_config_filename.copy()
    if external_module_config_paths:
        external_module_config_paths = external_module_config_paths.split(';')
        for external_module_config_path in external_module_config_paths:
            module_configs_by_config_filename = {
                **module_configs_by_config_filename,
                **load_config_directory(external_module_config_path)
            }

    # this only prepends the database_param_prefix to dictionary keys if parallel_db_name is not empty, which is never
    database_variables = False
    for project_config_entry in project_config.values():
        if project_config_entry.parallel_db_name != '':
            database_variables = True
            configs_by_config_filename_keys = list(configs_by_config_filename.keys())
            for key in configs_by_config_filename_keys:
                configs_by_config_filename[database_param_prefix + '_' + key] = configs_by_config_filename[key]
                del configs_by_config_filename[key]
            module_configs_by_config_filename_keys = list(module_configs_by_config_filename.keys())
            for key in module_configs_by_config_filename_keys:
                module_configs_by_config_filename[database_param_prefix + '_' + key] = module_configs_by_config_filename[key]
                del module_configs_by_config_filename[key]
            project_config_keys = list(project_config.keys())
            for key in project_config_keys:
                project_config[database_param_prefix + '_' + key] = project_config[key]
                del project_config[key]
            break

    config_filename_by_target_table = get_config_filename_by_target_table(module_configs_by_config_filename)

    return (config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables,
            project_config, mappings_by_mapping_filename)


def get_configs_mappings_from_data_mappings(project_group):
    configs_by_config_filename = {}
    mappings_by_mapping_filename = {}
    config_by_mapping_filename = {}
    project_config = {}

    for project in project_group:
        for data_mapping_group in project.data_mapping_groups:
            project_config[data_mapping_group.name] = data_mapping_group.mapping_group_config
            if not data_mapping_group.data_mappings:
                # to account for projects with no data mappings but with config, such as prisma
                # load in the ConfigEntry object by reading the config csv
                config = data_mapping_group.name
                if config not in configs_by_config_filename.keys():
                    configs_by_config_filename[config] = []  # read_config_csv - but no access to the config path
            else:
                for data_mapping in data_mapping_group.data_mappings:
                    config = data_mapping.config_entry

                    if config.key in configs_by_config_filename:
                        configs_by_config_filename[config.key] += [config]
                    else:
                        configs_by_config_filename[config.key] = [config]
                    mappings_by_mapping_filename[data_mapping.key] = data_mapping
                    config_by_mapping_filename[data_mapping.key] = config

                if data_mapping_group.programs:
                    for program_entry in data_mapping_group.programs:
                        programs_for_key = configs_by_config_filename.get(program_entry.key, [])
                        configs_by_config_filename[program_entry.key] = programs_for_key + [program_entry]
    return config_by_mapping_filename, configs_by_config_filename, project_config, mappings_by_mapping_filename
