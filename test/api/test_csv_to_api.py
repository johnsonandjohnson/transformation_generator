from transform_generator.api.api import app
from fastapi.testclient import TestClient
from transform_generator.api.adapter import adapter
from transform_generator.lib.config import get_config_structures
from transform_generator.generator.field import Field
from transform_generator.api.dto.field_dto import FieldDTO

import os
import unittest

client = TestClient(app)


class TestCsvToAPI(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Resources', 'positive_cases')

    # Test nested object validation
    def test_dto_load(self):

        """
        This test builds a Project Config hierarchical DTO structure from CSVs located in test folders.
        It submits the ProjectConfigDTO to a validation service to test validation of configured CSVs.
        """
        try:
            """ Load basic test resources """
            schema_path = os.path.join(self.resources_folder, 'schema')
            config_path = os.path.join(self.resources_folder, 'config')
            mapping_path = os.path.join(self.resources_folder, 'mapping')
            project_config_path_and_file = os.path.join(self.resources_folder, 'project_config',
                                                        'project_config_test.csv')

            """  DIGITAL CORE STRESS TEST 
            schema_path = '../../../cons_digital_core/generator/schema'
            config_path = '../../../cons_digital_core/generator/config'
            mapping_path = '../../../cons_digital_core/generator/mapping'
            project_config_path_and_file = '../../../cons_digital_core/generator/project_config/project_config.csv'
            """

            project_config_dto = adapter.dtos_from_csvs(
                project_config_path_and_file,
                config_path,
                mapping_path,
                schema_path
            )

            response = client.post("/config/validate", json=project_config_dto.dict())
            self.assertTrue(response is not None)
            self.assertEqual(response.status_code, 200)

        except Exception as ex:
            print(ex)
            raise ex

    # Test nested object validation
    def test_dto_to_business_structures(self):
        schema_path = os.path.join(self.resources_folder, 'schema')
        config_path = os.path.join(self.resources_folder, 'config')
        mapping_path = os.path.join(self.resources_folder, 'mapping')
        project_config_path_and_file = os.path.join(self.resources_folder, 'project_config', 'project_config_test.csv')

        # Use existing function that retrieves configs from file system as a baseline for the test.
        config_by_mapping_filename_orig, \
            config_filename_by_target_table_orig, \
            configs_by_config_filename_orig, \
            database_variables_orig, \
            project_config_orig, \
            mappings_by_mapping_filename_orig = get_config_structures(
                config_path,
                mapping_path,
                '',
                project_config_path_and_file
            )

        # Generate DTOs (sources from the same folder as the config structures above)
        project_config_dto = adapter.dtos_from_csvs(
            project_config_path_and_file,
            config_path,
            mapping_path,
            schema_path
        )

        # Now, use those DTOs to generate config structures from the alternate method.
        config_entry_by_mapping_filename, \
            config_filename_by_target_table, \
            config_entry_by_config_filename, \
            database_variables, \
            project_config_entry_by_filename, \
            mappings_by_mapping_filename, \
            table_definition_by_filename = adapter.get_config_structures_from_dtos(project_config_dto)

        # Lastly, compare the config structures generated from both methods.
        self.assertEqual(len(config_entry_by_mapping_filename), len(config_by_mapping_filename_orig))
        for key in config_entry_by_mapping_filename.keys():
            self.assertTrue(key in config_by_mapping_filename_orig)
            self.assertTrue(config_entry_by_mapping_filename[key].target_table,
                            config_by_mapping_filename_orig[key].target_table)

        # program types are not included in the response json
        num_program_types = 0
        num_program_only_config_files = 0
        for configs in configs_by_config_filename_orig.values():
            target_types = set()
            for config in configs:
                target_types.add(config.target_type)
                if config.target_type == 'program':
                    num_program_types += 1
            if target_types == {'program'}:
                num_program_only_config_files += 1

        self.assertEqual(len(config_filename_by_target_table),
                         len(config_filename_by_target_table_orig) - num_program_types)
        for key in config_filename_by_target_table.keys():
            self.assertTrue(key in config_filename_by_target_table_orig)

        self.assertEqual(len(config_entry_by_config_filename),
                         len(configs_by_config_filename_orig) - num_program_only_config_files)
        for key in config_entry_by_config_filename.keys():
            self.assertTrue(key in configs_by_config_filename_orig)

        self.assertEqual(len(project_config_entry_by_filename),
                         len(project_config_orig) - num_program_only_config_files)

        self.assertEqual(len(mappings_by_mapping_filename), len(mappings_by_mapping_filename_orig))
        for key in mappings_by_mapping_filename.keys():
            self.assertTrue(key in mappings_by_mapping_filename_orig)

        self.assertEqual(database_variables, database_variables_orig)

    def test_field_dto_conversion(self):
        field1 = Field('name', 'INT', 'NOT NULL', 'test')
        fieldDto1 = FieldDTO.from_business_object(field1)

        self.assertFalse(fieldDto1.nullable)

        field2 = Field('name', 'INT', 'NULL', 'test')
        fieldDto2 = FieldDTO.from_business_object(field2)

        self.assertTrue(fieldDto2.nullable)
