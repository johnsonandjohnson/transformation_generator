import unittest
import os

from transform_generator.lib.table_definition import TableDefinition
from transform_generator.reader.table_definition_reader import read_table_definition_csv


class TestTableDefinition(unittest.TestCase):

    def test_table_definition_reader(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'schema', 'schema_definition_test_file')
        test_table_definition = read_table_definition_csv(file_name)
        test_table_definition_file = test_table_definition.fields

        expected_column_name = "column name"
        expected_column_nullable = "NULL"
        expected_column_data_type = "TIMESTAMP"
        expected_column_data_length = 0
        expected_column_data_scale = 0
        for key, value in test_table_definition_file.items():
            self.assertEqual(test_table_definition_file.get(key).name, expected_column_name)
            self.assertEqual(test_table_definition_file.get(key).data_type, expected_column_data_type)
            self.assertEqual(test_table_definition_file.get(key).nullable, expected_column_nullable)
            self.assertEqual(test_table_definition_file.get(key).scale, expected_column_data_scale)
            self.assertEqual(test_table_definition_file.get(key).precision, expected_column_data_length)

    def test_empty_definition_file(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'schema', 'schema_definition_empty_file')

        with self.assertRaises(ValueError) as raises:
            test_table_definition = read_table_definition_csv(file_name)


