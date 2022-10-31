import os
import tempfile
import unittest

from transform_generator.regression_test import generate_and_compare


class TestRegressionTest(unittest.TestCase):
    pos_cases_project_config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                'Resources/positive_cases/project_config')
    expected_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources/output')
    project_base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')

    def test_regression_positive_case(self):
        with tempfile.TemporaryDirectory() as temp_actual_output_dir:
            result = generate_and_compare(self.pos_cases_project_config_dir, temp_actual_output_dir,
                                          self.expected_output_dir, self.project_base_dir)
            self.assertEqual(result, True)
