from itertools import groupby
from os import listdir
from os.path import join, exists, isfile, split

from transform_generator.data_mapping_group import DataMappingGroup
from transform_generator.lib.config import get_config_by_mapping_filename
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.logging import get_logger
from transform_generator.lib.mapping import load_mappings
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.plugin.project_loader import ProjectLoader
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.project import Project
from transform_generator.reader.config_reader import read_config_csv
from transform_generator.reader.project_config_reader import read_project_config_csvs
from transform_generator.reader.table_definition_reader import read_table_definition


logger = get_logger(__name__)


class DcProjectLoader(ProjectLoader):

    def __init__(self):
        self._current_proj_cfg_entry = None

    def load_project_group(self, project_group_config_path: str, project_base_path: str) -> list[Project]:
        """
        Read all entries from specified project group config file and load all projects described therein.
        Each projects path is the project base path joined with the project name as defined in the config file. Each
        project is loaded into a Project object, and used to construct a ProjectGroup object
        .
        @param project_config_path: The full path to the project configuration file.
        @param project_base_path: The path to the project base directory, which contains all project directories.
        @return: A Project Group containing all of the configured projects.
        """
        logger.info('Loading project group from provide')
        cfg_entries = read_project_config_csvs(project_group_config_path.split(';'))


        # Exclude projects which have no 'config' folder
        filtered_mapping_group_cfg_entries = (e for e in cfg_entries.values()
                                              if exists(join(project_base_path, e.group_name, 'config')))

        # Must be sorted by group name for the groupby function
        sorted_mapping_group_cfg_entries = sorted(filtered_mapping_group_cfg_entries, key=lambda x: x.group_name)

        entries_by_group_name = groupby(sorted_mapping_group_cfg_entries, key=lambda x: x.group_name)

        projects = []
        for group_name, cfg_entries in entries_by_group_name:
            projects.append(self.load_project(join(project_base_path, group_name), group_name, cfg_entries))
        return projects

    def load_project(self, project_path: str,
                     name: str,
                     project_config_entries: list[ProjectConfigEntry],
                     ) -> Project:
        mapping_config_path = self.construct_directory_path(project_path, 'config')

        proj_cfg_by_filename = {entry.config_filename: entry for entry in project_config_entries}

        data_mapping_groups = []
        for filename in listdir(mapping_config_path):
            # filter out directories
            if isfile(join(mapping_config_path, filename)):
                config_file_path = join(mapping_config_path, filename)
                if not proj_cfg_by_filename or (proj_cfg_by_filename and filename in config_file_path):
                    proj_cfg_entry = proj_cfg_by_filename.get(filename, None)
                    config_entries = read_config_csv(config_file_path)
                    data_mapping_group = self.load_mapping_group(project_path, config_file_path, config_entries,
                                                                 proj_cfg_entry)
                    programs = self.load_programs(config_entries)
                    if programs:
                        data_mapping_group.programs = programs
                    data_mapping_groups.append(data_mapping_group)

        return Project(name, data_mapping_groups)

    def load_mapping_group(self, project_path: str,
                           mapping_group_config_file_path: str,
                           config_entries: list[ConfigEntry],
                           mapping_group_config: ProjectConfigEntry = None) -> DataMappingGroup:
        configs_by_config_filename = {}
        for entry in config_entries:
            entries = configs_by_config_filename.get(entry.key, list())
            entries.append(entry)
            configs_by_config_filename[entry.key] = entries

        config_by_mapping_filename = get_config_by_mapping_filename(configs_by_config_filename)
        mappings_by_filename = {}
        ordered_mappings = []
        if config_by_mapping_filename:
            mapping_directory = self.construct_directory_path(project_path, 'mapping')
            mapping_filenames = list(config_by_mapping_filename.keys())

            mappings_by_filename = load_mappings(mapping_directory, mapping_filenames)

            for config_entry in config_entries:
                if config_entry.target_type != 'program':
                    for input_file in config_entry.input_files:
                        mapping = mappings_by_filename[input_file]
                        mapping.config_entry = config_entry

                        # The mappings should be in the same order as the config entries in the file
                        # to match previous behavior on digital core.
                        ordered_mappings.append(mapping)

        schema_directory = self.construct_directory_path(project_path, 'schema')

        for mapping in ordered_mappings:
            table_path = join(schema_directory, mapping.config_entry.target_table)
            mapping.table_definition = read_table_definition(table_path, db_name=mapping.database_name)

        parent_dir, proj_dir = split(mapping_group_config_file_path)
        if proj_dir.lower() != 'generator':
            name = proj_dir
        else:
            grand_parent_dir, parent_dir = split(parent_dir)
            name = parent_dir

        return DataMappingGroup(
            name=name,
            data_mappings=ordered_mappings,
            mapping_group_config=mapping_group_config
        )

    def load_mapping(self, mapping_path: str) -> DataMapping:
        return super().load_mapping(mapping_path)

    def load_table_definition(self, table_definition_path: str) -> TableDefinition:
        return super().load_table_definition(table_definition_path)

    def load_programs(self, config_entries: list[ConfigEntry]) -> list[ConfigEntry]:
        program_entries = []
        for entry in config_entries:
            if entry.target_type == 'program':
                program_entries.append(entry)

        return program_entries

    def construct_directory_path(self, project_path, directory_name):
        if exists(join(project_path, 'generator')):
            return join(project_path, 'generator', directory_name)
        elif exists(join(project_path, directory_name)):
            return join(project_path, directory_name)
        raise Exception(f'Could not locate {directory_name} in {project_path} ')


