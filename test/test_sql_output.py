import unittest
import os

from transform_generator.generate_sql_output import generate_select_queries
from transform_generator.lib.config import get_config_structures
from transform_generator.lib.sql_scripts import generate_sql_scripts


class TestSqlOutput(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'positive_cases')
    schema_path = os.path.join(resources_folder, 'schema')
    config_path = os.path.join(resources_folder, 'config')
    mapping_path = os.path.join(resources_folder, 'mapping')
    project_config_path = os.path.join(resources_folder, 'project_config', 'project_config_test.csv')
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_path, '',
                                                                             project_config_path)

    def _get_data(self):
        for configs in self.config_by_mapping_filename.values():
            target_language = configs.target_language.lower()
            target_table = configs.target_table
            queries = generate_select_queries([configs], self.mappings_by_mapping_filename, self.schema_path,
                                              self.project_config, self.config_filename_by_target_table)
            if queries:
                script_cells_by_target_table = generate_sql_scripts(queries, self.project_config,
                                                                    self.config_filename_by_target_table, self.schema_path)

                query = script_cells_by_target_table[target_table][0]
                return target_language, target_table, query

    def test_query_and_target_language(self):
        target_language, target_table, query = self._get_data()
        self.assertEqual(target_language, 'databricks')
        self.assertIn(target_table, query)