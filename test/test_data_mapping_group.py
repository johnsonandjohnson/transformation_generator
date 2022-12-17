import os
import unittest
from os.path import join

from transform_generator.lib import mapping, config
from transform_generator.data_mapping_group import DataMappingGroup
from transform_generator.plugin.loader import get_plugin_loader
from transform_generator.reader.config_reader import read_config_csv


class TestMappingGroup(unittest.TestCase):
    pos_cases_resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources/positive_cases')

    def test_load_mapping_group(self):
        mapping_group_config_path = join(TestMappingGroup.pos_cases_resources_folder,
                                         'config',
                                         'config_test_file_for_mappings.csv')

        config_entries = read_config_csv(mapping_group_config_path)
        proj_grp_loader = get_plugin_loader().project_group_loader()
        project: DataMappingGroup = proj_grp_loader.load_mapping_group(TestMappingGroup.pos_cases_resources_folder,
                                                       mapping_group_config_path, config_entries)

        config_entries = config.read_config_csv(mapping_group_config_path)
        configs_by_config_filename = {}
        for entry in config_entries:
            entries = configs_by_config_filename.get(entry.key, list())
            entries.append(entry)
            configs_by_config_filename[entry.key] = entries

        config_by_mapping_filename = config.get_config_by_mapping_filename(configs_by_config_filename)

        mapping_path = join(TestMappingGroup.pos_cases_resources_folder, 'mapping')
        mappings_by_mapping_filename = mapping.load_mappings(mapping_path,
                                                             list(config_by_mapping_filename.keys()))
        for config_entry in config_by_mapping_filename.values():
            for input_file in config_entry.input_files:
                mappings_by_mapping_filename[input_file].config_entry = config_entry

        self.assertEqual(sorted(mappings_by_mapping_filename.values()), sorted(project.data_mappings))
        self.assertEqual(None, project.mapping_group_config)

        for data_mapping in project.data_mappings:
            self.assertEqual(mappings_by_mapping_filename[data_mapping.key], data_mapping)
