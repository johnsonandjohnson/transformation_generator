import os
import unittest

from transform_generator.project_group import load_project_group


class TestProjectGroup(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')

    def test_project_group(self):
        project_config_path = os.path.join(TestProjectGroup.resources_folder,
                                           'positive_cases',
                                           'project_config',
                                           'project_config_test.csv')

        project_group = load_project_group(project_config_path, TestProjectGroup.resources_folder)

        print(project_group)
