import os
import unittest
from os import listdir
from os.path import join, basename

from transform_generator.data_mapping_group import load_mapping_group, load_programs
from transform_generator.project import load_project
from transform_generator.reader.config_reader import read_config_csv


class TestProject(unittest.TestCase):
    pos_cases_resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources/positive_cases')

    def test_project(self):
        project = load_project(TestProject.pos_cases_resources_folder, "test_name")

        config_dir = join(TestProject.pos_cases_resources_folder, 'config')
        config_files = (join(config_dir, config_file) for config_file in listdir(config_dir))

        data_mapping_groups = []
        for config_file in config_files:
            config_entries = read_config_csv(config_file)
            data_mapping_group = load_mapping_group(TestProject.pos_cases_resources_folder, config_file, config_entries)
            programs = load_programs(config_entries)
            if programs:
                data_mapping_group.programs = programs
            data_mapping_groups.append(data_mapping_group)

        self.assertEqual(sorted(project.data_mapping_groups), sorted(data_mapping_groups))
        self.assertEqual(project.name, 'test_name')
