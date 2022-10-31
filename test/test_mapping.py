import unittest
import os
from transform_generator.lib.mapping import load_mapping
from transform_generator.lib.data_mapping import DataMapping

class TestMapping(unittest.TestCase):
    pos_cases_resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'positive_cases')
    neg_cases_resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases')

    def test_mapping(self):
        test_file = 'mapping_test_file_case_statement.csv'
        mapping_sheet_path = os.path.join(TestMapping.pos_cases_resources_folder, 'mapping', test_file)

        result = load_mapping(mapping_sheet_path)
        self.assertIsInstance(result, tuple)
        self.assertIsInstance(result[1], DataMapping)
        self.assertEqual(result[0], test_file)

    def test_mapping_from_clause(self):
        test_file = 'mapping_test_file_group_by.csv'
        mapping_sheet_path = os.path.join(self.pos_cases_resources_folder, 'mapping', test_file)

        result = load_mapping(mapping_sheet_path)
        self.assertIsInstance(result, tuple)
        self.assertIsInstance(result[1], DataMapping)
        self.assertEqual(result[0], test_file)

    def test_mapping_tgt_db_name_mismatch(self):
        test_file = 'mapping_test_file_case_statement_tgt_db_modified.csv'
        mapping_sheet_path = os.path.join(self.neg_cases_resources_folder, 'mapping', test_file)

        with self.assertRaises(Exception) as context:
            load_mapping(mapping_sheet_path)
        self.assertTrue('Target database and table need to be same for all rules in a given sheet' in str(context.exception))

    def test_mapping_src_table_database_mismatch(self):
        test_file = 'mapping_test_file_case_statement_src_tbl_db_modified.csv'
        mapping_sheet_path = os.path.join(self.neg_cases_resources_folder, 'mapping', test_file)

        with self.assertRaises(Exception) as context:
            load_mapping(mapping_sheet_path)
        self.assertTrue('Different Source Tables/Databases and no specified from/join clause' in str(context.exception))

    def test_mapping_syntax_error_business_rule(self):
        test_file = 'mapping_test_file_case_statement_syntax_error_business_rule.csv'
        mapping_sheet_path = os.path.join(self.neg_cases_resources_folder, 'mapping', test_file)

        with self.assertRaises(Exception) as context:
            load_mapping(mapping_sheet_path)
        self.assertTrue('mismatched input' in str(context.exception))

    def test_mapping_syntax_error_transform_rule(self):
        test_file = 'mapping_test_file_case_statement_syntax_error_transform_rule.csv'
        mapping_sheet_path = os.path.join(self.neg_cases_resources_folder, 'mapping', test_file)

        with self.assertRaises(Exception) as context:
            load_mapping(mapping_sheet_path)
        self.assertTrue('mismatched input' in str(context.exception))
