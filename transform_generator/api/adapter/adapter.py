import logging

from typing import List, Dict, Tuple

from transform_generator.reader.table_definition_reader import get_table_definition
from transform_generator.lib.config import get_config_structures
from transform_generator.api.dto.config_entry_dto import ConfigEntryDTO
from transform_generator.api.dto.config_entry_group_dto import ConfigEntryGroupDTO
from transform_generator.api.dto.data_mapping_dto import DataMappingDTO
from transform_generator.api.dto.project_config_dto import ProjectConfigDTO
from transform_generator.api.dto.table_definition_dto import TableDefinitionDTO
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.table_definition import TableDefinition

logger = logging.getLogger(__name__)


def convert_config_entries_to_dtos(
        config_key: str,
        config_entries: List[ConfigEntry],
        mappings_by_mapping_filename,
        table_definitions_by_database_table
) -> List[ConfigEntryDTO]:
    """
    Constructs a list of ConfigEntryDTOs and child objects based on dictionaries.

    @param config_key: A key identifying the configuration entry group (eg: spreadsheet file name)
    @param config_entries: List of ConfigEntry business objects.
    @param mappings_by_mapping_filename: Dictionary that associates a mapping file name to DataMapping
    @param table_definitions_by_database_table Dictionary that associates a database table name to the definition.
    @return: A ProjectConfigDTO constructed from the CSVs in folders.
    """
    config_entry_dtos = []

    # Loop for each row in the config file.
    for config_entry in config_entries:
        # Construct data mappings
        data_mapping_dtos = []
        for input_file in config_entry.input_files:
            if input_file not in mappings_by_mapping_filename:
                logger.error('Unable to find data mapping sheet [%s] referenced by [%s]. Skipping this ConfigEntry.',
                             input_file, config_key)
                continue

            data_mapping_dtos.append(
                DataMappingDTO.from_business_object(
                    input_file,
                    mappings_by_mapping_filename[input_file]
                )
            )

        # Build the table definition object from the schema files.
        # NOTE: config_entry.target_table takes the form of [database].[table_name]
        if config_entry.target_table not in table_definitions_by_database_table:
            logger.error('Unable to find schema [%s] referenced by [%s]. Skipping this ConfigEntry.',
                         config_entry.target_table, config_key)
            continue

        table_definition_dto = TableDefinitionDTO.from_business_object(
            config_entry.target_table,
            table_definitions_by_database_table[config_entry.target_table],
            data_mapping_dtos
        )
        config_entry_dtos.append(
            ConfigEntryDTO.from_business_object(
                config_key,
                config_entry,
                table_definition_dto
            )
        )
    return config_entry_dtos


def dtos_from_csvs(
        project_config_path_and_file: str,
        config_path: str,
        mapping_path: str,
        schema_path: str) -> ProjectConfigDTO:
    """
    Constructs a Project Config DTO hierarchy from specified CSV files.

    @param project_config_path_and_file: Location of an overall project (aka: generator) config file.
    @param config_path: Location of a folder that stores configuration CSV files.
    @param mapping_path: Location of a folder that stores data mapping CSV files.
    @param schema_path: Location of a folder that stores table schema files.
    @return: A ProjectConfigDTO constructed from the CSVs in folders.
    """
    config_by_mapping_filename, \
        config_filename_by_target_table, \
        configs_by_config_filename, \
        database_variables, \
        project_config, \
        mappings_by_mapping_filename = get_config_structures(
            config_path,
            mapping_path,
            '',
            project_config_path_and_file
        )

    # Build the Table Definitions upfront, so they can be referenced when building ConfigEntry objects
    table_keys = {
        (data_mapping.database_name, data_mapping.table_name, data_mapping.table_name_qualified)
        for data_mapping in mappings_by_mapping_filename.values()
    }
    table_definitions_by_database_table = {
        table_name_qualified: get_table_definition(schema_path, db_name, table_name)
        for db_name, table_name, table_name_qualified in table_keys
    }

    config_entry_groups = []
    # Loop for each config file referenced by overall project config.
    for config_key in project_config:
        if config_key not in configs_by_config_filename:
            logger.error('Unable to find configuration [%s] referenced by project config. Skipping...', config_key)
            continue

        config_entries = configs_by_config_filename[config_key]
        config_entry_dtos = convert_config_entries_to_dtos(
            config_key,
            config_entries,
            mappings_by_mapping_filename,
            table_definitions_by_database_table
        )

        if len(config_entry_dtos):
            config_entry_groups.append(
                ConfigEntryGroupDTO.from_business_object(
                    project_config[config_key],
                    config_entry_dtos
                )
            )

    project_config_dto = ProjectConfigDTO(key=project_config_path_and_file, config_entry_groups=config_entry_groups)
    return project_config_dto


def get_config_structures_from_dtos(project_config: ProjectConfigDTO) -> \
        Tuple[
            Dict[str, ConfigEntry],
            Dict[str, str],
            Dict[str, List[ConfigEntry]],
            bool,
            Dict[str, ProjectConfigEntry],
            Dict[str, DataMapping],
            Dict[str, TableDefinition]
        ]:
    """
    Given a ProjectConfigDTO, returns a set of dictionaries in the format expected by config generator processes.
    (See existing get_config_structures function under lib/config.py)

    @param project_config:  A ProjectConfigDTO which represents a transformation generator project.
    @return:    A tuple of dictionaries mapping to the existing lib/config.py:get_config_structures
    """
    config_entry_by_mapping_filename = {}
    config_filename_by_target_table = {}
    config_entry_by_config_filename = {}
    database_variables = False
    project_config_entry_by_filename = {}
    mappings_by_mapping_filename = {}
    table_definitions_by_table_name = {}

    # Map the config entry groups to project_config_entry objects.
    for config_entry_group_dto in project_config.config_entry_groups:
        if config_entry_group_dto.parallel_db_name != '':
            database_variables = True
        project_config_entry_by_filename[config_entry_group_dto.key] = config_entry_group_dto.to_business_object()

        for config_entry_dto in config_entry_group_dto.config_entries:
            config_entry = config_entry_dto.to_business_object()

            # Config filename by target table
            qualified_table_name = config_entry_dto.table_definition.qualified_table_name()
            config_filename_by_target_table[qualified_table_name] = config_entry_group_dto.key

            # Config entries by config filename
            config_entry_by_config_filename[config_entry_group_dto.key] = config_entry

            table_definitions_by_table_name[qualified_table_name] = \
                config_entry_dto.table_definition.to_business_object()

            for data_mapping_dto in config_entry_dto.table_definition.data_mappings:
                # Config entries mapped by data mapping filename
                config_entry_by_mapping_filename[data_mapping_dto.key] = config_entry
                data_mapping = data_mapping_dto.to_business_object(config_entry_dto.table_definition)
                data_mapping.config_entry = config_entry
                mappings_by_mapping_filename[data_mapping_dto.key] =\
                    data_mapping_dto.to_business_object(config_entry_dto.table_definition)

    return config_entry_by_mapping_filename, \
        config_filename_by_target_table, \
        config_entry_by_config_filename, \
        database_variables, \
        project_config_entry_by_filename, \
        mappings_by_mapping_filename, \
        table_definitions_by_table_name
