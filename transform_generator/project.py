from dataclasses import dataclass
from os import listdir
from os.path import join, isfile
from typing import List, Sequence

from transform_generator.data_mapping_group import DataMappingGroup, load_mapping_group, construct_directory_path, load_programs
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.reader.config_reader import read_config_csv


@dataclass
class Project:
    name: str
    data_mapping_groups: List[DataMappingGroup]


def load_project(project_path: str,
                 name: str,
                 project_config_entries: Sequence[ProjectConfigEntry] = list()) -> Project:
    mapping_config_path = construct_directory_path(project_path, 'config')

    proj_cfg_by_filename = {entry.config_filename: entry for entry in project_config_entries}

    data_mapping_groups = []
    for filename in listdir(mapping_config_path):
        # filter out directories
        if isfile(join(mapping_config_path, filename)):
            config_file_path = join(mapping_config_path, filename)
            if not proj_cfg_by_filename or (proj_cfg_by_filename and filename in config_file_path):
                proj_cfg_entry = proj_cfg_by_filename.get(filename, None)
                config_entries = read_config_csv(config_file_path)
                data_mapping_group = load_mapping_group(project_path, config_file_path, config_entries, proj_cfg_entry)
                programs = load_programs(config_entries)
                if programs:
                    data_mapping_group.programs = programs
                data_mapping_groups.append(data_mapping_group)

    return Project(name, data_mapping_groups)
