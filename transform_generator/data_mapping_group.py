from os.path import join, split, exists
from typing import List
from dataclasses import dataclass, field

from transform_generator.lib import config
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.mapping import load_mappings
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.reader.config_reader import read_config_csv
from transform_generator.reader.table_definition_reader import read_table_definition


@dataclass(order=True)
class DataMappingGroup:
    name: str
    data_mappings: List[DataMapping]
    tables_by_db_table_name: dict[str, TableDefinition]
    mapping_group_config: ProjectConfigEntry = None
    programs: List[ConfigEntry] = field(default_factory=list)


def load_mapping_group(project_path: str,
                       mapping_group_config_file_path: str,
                       config_entries: List[ConfigEntry],
                       mapping_group_config: ProjectConfigEntry = None) -> DataMappingGroup:
    configs_by_config_filename = {}
    for entry in config_entries:
        entries = configs_by_config_filename.get(entry.key, list())
        entries.append(entry)
        configs_by_config_filename[entry.key] = entries

    config_by_mapping_filename = config.get_config_by_mapping_filename(configs_by_config_filename)
    mappings_by_filename = {}

    if config_by_mapping_filename:
        mapping_directory = construct_directory_path(project_path, 'mapping')
        mapping_filenames = list(config_by_mapping_filename.keys())

        mappings_by_filename = load_mappings(mapping_directory, mapping_filenames)

        for config_entry in config_entries:
            if config_entry.target_type != 'program':
                for input_file in config_entry.input_files:
                    mappings_by_filename[input_file].config_entry = config_entry

    schema_directory = construct_directory_path(project_path, 'schema')
    table_file_names = (join(schema_directory, entry.target_table)
                        for entry in config_by_mapping_filename.values())
    table_defs = (read_table_definition(table_path) for table_path in table_file_names)
    tables_by_db_table_name = {t.table_name: t for t in table_defs}

    parent_dir, proj_dir = split(mapping_group_config_file_path)
    if proj_dir.lower() != 'generator':
        name = proj_dir
    else:
        grand_parent_dir, parent_dir = split(parent_dir)
        name = parent_dir

    return DataMappingGroup(
        name=name,
        data_mappings=sorted(mappings_by_filename.values()),
        tables_by_db_table_name=tables_by_db_table_name,
        mapping_group_config=mapping_group_config
    )


def load_programs(config_entries: List[ConfigEntry]) -> List[ConfigEntry]:
    program_entries = []
    for entry in config_entries:
        if entry.target_type == 'program':
            program_entries.append(entry)

    return program_entries


def construct_directory_path(project_path, directory_name):
    if exists(join(project_path, 'generator')):
        return join(project_path, 'generator', directory_name)
    elif exists(join(project_path, directory_name)):
        return join(project_path, directory_name)
    raise Exception('Could not locate ' + directory_name + ' directory')
