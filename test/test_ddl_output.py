import filecmp
import os
import tempfile
import unittest

from transform_generator.DdlPathParameters import DdlPathParameters
from transform_generator.generate_ddl_output import generate_ddl_output


class TestDdlOutput(unittest.TestCase):
    pos_cases_resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'positive_cases')
    local_output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'output',
                                       'positive_cases')

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_output_filepath = self.tmp_dir.name

    def tearDown(self):
        self.tmp_dir.cleanup()

    @staticmethod
    def _get_file_paths(dir_path):
        return {os.path.join(os.path.relpath(path, dir_path), file)
                for path, directory, files in os.walk(dir_path)
                for file in files}

    @staticmethod
    def _compare_each_file(common_file_set, local_dir, tmp_dir):
        return [(os.path.join(local_dir, file), os.path.join(tmp_dir, file))
                for file in common_file_set
                if not filecmp.cmp(os.path.join(local_dir, file), os.path.join(tmp_dir, file), shallow=False)]

    def _compare_files(self, tmp_dir, local_dir, tmp_set, local_set):
        common_file_set = local_set.intersection(tmp_set)
        if len(common_file_set) == len(tmp_set):
            return self._compare_each_file(common_file_set, local_dir, tmp_dir)
        else:
            return f'common_file_set: {common_file_set} does not match tmp_set: {tmp_set}'

    def test_ddl_output(self):
        schema_path = os.path.join(self.pos_cases_resources_folder, 'schema')
        config_path = os.path.join(self.pos_cases_resources_folder, 'config')
        mapping_sheet_path = os.path.join(self.pos_cases_resources_folder, 'mapping')
        project_config_path = os.path.join(self.pos_cases_resources_folder, 'project_config',
                                             'project_config_test.csv')
        output_databricks = self.tmp_output_filepath
        output_datafactory = self.tmp_output_filepath
        databricks_folder_location = '/Shared/cicd'
        external_module_config_paths = ''

        test_params = DdlPathParameters(config_path, schema_path, mapping_sheet_path, project_config_path,
                                        output_databricks, output_datafactory, databricks_folder_location,
                                        external_module_config_paths)

        generate_ddl_output(test_params)

        tmp_file_set = self._get_file_paths(self.tmp_output_filepath)
        local_file_set = self._get_file_paths(self.local_output_folder)
        
        file_comparison = self._compare_files(self.tmp_output_filepath, self.local_output_folder, tmp_file_set,
                                              local_file_set)
        self.assertFalse(bool(file_comparison), f'Mismatching files found: {file_comparison}')
