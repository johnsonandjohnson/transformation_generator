import unittest
import os

from transform_generator.reader.config_reader import read_config_csv


class TestConfigReader(unittest.TestCase):
    def test_csv_reader(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'config', 'config_test_file.csv')

        config_file = read_config_csv(file_name)
        config_file_actual = config_file[0]

        expected_target_table = "trgt_table"
        expected_target_type = "trgt_type"
        expected_target_language = "impala"
        expected_input_files = ['input_file_1', 'input_file_2']
        expected_dependencies = "dependency_abc"
        expected_tuning_parameter = ['tuning_prmtr_1', 'tning_prmtr_2']
        expected_load_type = "full"

        self.assertEqual(config_file_actual.target_table, expected_target_table)
        self.assertEqual(config_file_actual.target_type, expected_target_type)
        self.assertEqual(config_file_actual.target_language, expected_target_language)
        self.assertListEqual(config_file_actual.input_files, expected_input_files)
        self.assertEqual(config_file_actual.dependencies, expected_dependencies)
        self.assertListEqual(config_file_actual.tuning_parameter, expected_tuning_parameter)
        self.assertEqual(config_file_actual.load_type, expected_load_type)

    def test_csv_reader_no_data(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'config', 'config_test_file_no_data.csv')
        expected_data = ('Empty Config File : ' + file_name,)
        with self.assertRaises(ValueError) as raises:
            config_file = read_config_csv(file_name)
        self.assertTupleEqual(raises.exception.args, expected_data)

    def test_csv_reader_missing_work_stream_field(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'config', 'config_test_file_workstream_missing.csv')
        expected_data = ('Work_stream',)
        with self.assertRaises(KeyError) as raises:
            config_file = read_config_csv(file_name)
        self.assertTupleEqual(raises.exception.args, expected_data)

    def test_csv_reader_no_data_no_fields(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'config', 'config_test_file_no_data_no_fields.csv')
        expected_data = ('Empty Config File : ' + file_name ,)
        with self.assertRaises(ValueError) as raises:
            config_file = read_config_csv(file_name)
        self.assertTupleEqual(raises.exception.args, expected_data)

    def test_csv_reader_missing_work_stream_field(self):
        file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'negative_cases', 'config', 'config_test_file_not_found.csv')
        expected_data = (2, 'No such file or directory',)
        with self.assertRaises(FileNotFoundError) as raises:
            config_file = read_config_csv(file_name)
        self.assertTupleEqual(raises.exception.args, expected_data)