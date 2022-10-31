import unittest
import os

from transform_generator.generator.generator_error import GeneratorException
from transform_generator.lib.sql_scripts import generate_script_insert
from transform_generator.lib.config import get_config_structures
from transform_generator.generator.transform_gen import generate_select_query
from transform_generator.reader.table_definition_reader import get_table_definition


class TestTransformGen(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')
    schema_path = os.path.join(resources_folder, 'positive_cases', 'schema')
    config_path = os.path.join(resources_folder, 'positive_cases', 'config')
    mapping_path = os.path.join(resources_folder, 'positive_cases', 'mapping')
    project_config_path = os.path.join(resources_folder, 'positive_cases', 'project_config', 'project_config_test.csv')
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_path, '',
                                                                             project_config_path)

    for mapping_filename, config_entry in config_by_mapping_filename.items():
        if mapping_filename == 'mapping_test_file_simple_transform_gen.csv':
            def test_simple_transform_gen(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_simple_transform_gen.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.SIMPLE_TRANSFORM_GEN\n"
                expected += "SELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2\n" \
                            "FROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

            def test_simple_transform_gen_optional_params(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_simple_transform_gen.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                select_query = generate_select_query(data_mapping, table_definition)

                target_table = self.config_entry.target_table
                insert_query = generate_script_insert('DATABRICKS', data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.SIMPLE_TRANSFORM_GEN\n"
                expected += "SELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2\n" \
                            "FROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_case_statement.csv':
            def test_case_statement(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_case_statement.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.CASE_STATEMENT\n"
                expected += "SELECT\n\tCASE SRC_TBL1.SRC_COL1\n\tWHEN '01' THEN 1\n\tELSE 0\n\tEND AS TGT_COL1," \
                            "\n\tCASE SUBSTR(SRC_COL1, 6, 2)\n\tWHEN '01' THEN 1\n\tELSE 0\n\tEND AS TGT_COL2," \
                            "\n\tSRC_TBL1.SRC_COL1 AS TGT_COL3," \
                            "\n\tSRC_TBL1.SRC_COL1 AS TGT_COL4," \
                            "\n\tCAST(SRC_TBL1.SRC_COL1 AS STRING) AS TGT_COL5" \
                            "\nFROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_is_null.csv':
            def test_is_null(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_is_null.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.IS_NULL"
                expected += "\nSELECT\n\tIF(SRC_TBL1.SRC_COL1 IS NULL, 0, 1) AS TGT_COL1\nFROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_where_clause.csv':
            def test_where_clause(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_where_clause.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.WHERE_CLAUSE"
                expected += "\nSELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2,"
                expected += "\n\tSRC_TBL1.SRC_COL3 AS TGT_COL3\nFROM SRC_DB1.SRC_TBL1\nWHERE SRC_TBL1.SRC_COL1 = 'A'"
                expected += " AND SRC_TBL1.SRC_COL2 <> 'ABC' AND SRC_TBL1.SRC_COL3 NOT IN ('ABC', 'DEF');"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_implicit_cast.csv':
            def test_implicit_cast(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_implicit_cast.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.IMPLICIT_CAST"
                expected += "\nSELECT\n\tCAST(SRC_TBL1.SRC_COL1 AS INT) AS TGT_COL1\nFROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_in_clause.csv':
            def test_in_clause(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_in_clause.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])


                expected = "INSERT OVERWRITE TEST.IN_CLAUSE"
                expected += "\nSELECT\n\tIF(SRC_TBL1.SRC_COL1 IN ('A', 'C'), 0, 1) AS TGT_COL1\nFROM SRC_DB1.SRC_TBL1;"
                self.assertEqual(expected, insert_query)

        if mapping_filename == 'mapping_test_file_group_by.csv':
            def test_group_by(self):
                data_mapping = self.mappings_by_mapping_filename['mapping_test_file_group_by.csv']
                table_definition = get_table_definition(self.schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)
                target_language = self.config_entry.target_language.lower()
                target_table = self.config_entry.target_table
                select_query = generate_select_query(data_mapping, table_definition, target_language,
                                                     self.project_config, self.config_filename_by_target_table,
                                                     self.config_entry.load_type)

                insert_query = generate_script_insert(target_language, data_mapping,
                                                      table_definition, select_query, True,
                                                      self.project_config,
                                                      self.config_filename_by_target_table[target_table])

                expected = "INSERT OVERWRITE TEST.GROUP_BY"
                expected += "\nSELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1\nFROM SRC_DB1.SRC_TBL1\nGROUP BY SRC_COL1;"
                self.assertEqual(expected, insert_query)
