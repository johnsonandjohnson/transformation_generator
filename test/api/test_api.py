from transform_generator.api.api import app
from fastapi.testclient import TestClient

import unittest
import json

client = TestClient(app)


class TestAPI(unittest.TestCase):
    def test_api_validation_success(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })
        self.assertEqual(response.status_code, 200)

    def test_api_validation_success_no_mappings(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string"
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })
        self.assertEqual(response.status_code, 200)

    def test_api_fail_no_config_entry_group(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": []
        })

        self.assertEqual(response.status_code, 422)

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'][1], 'config_entry_groups')
        self.assertEqual(response_json['detail'][0]['msg'], 'ensure this value has at least 1 items')

    def test_api_fail_no_config_entry(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": []
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups', 0, 'config_entries'])
        self.assertEqual(response_json['detail'][0]['msg'], 'ensure this value has at least 1 items')

    def test_api_fail_no_table_definition(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "data_mappings": [
                                {
                                    "key": "string",
                                    "transformation_expressions": [
                                        {
                                            "target_column_name": "TGT_COL1",
                                            "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                        }
                                    ]
                                }
                            ],
                            "table_definition": {},
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 3)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition',
                                                             'database_name'])
        self.assertEqual(response_json['detail'][1]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition',
                                                             'fields'])
        self.assertEqual(response_json['detail'][2]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition',
                                                             'table_name'])
        self.assertEqual(response_json['detail'][0]['msg'], 'field required')
        self.assertEqual(response_json['detail'][1]['msg'], 'field required')
        self.assertEqual(response_json['detail'][2]['msg'], 'field required')

    def test_api_fail_dependencies_invalid(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "string",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "some value",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'dependencies'])
        self.assertEqual(response_json['detail'][0]['msg'], 'Must be empty unless target_type is program. Value was '
                                                            '[some value] for target_type [table] in group '
                                                            '[string].')

    def test_api_fail_target_invalid(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "something",
                            "target_language": "something",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "string",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": 1,
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 3)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'target_type'])
        self.assertEqual(response_json['detail'][1]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'target_language'])
        self.assertEqual(response_json['detail'][2]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'load_type'])
        self.assertTrue('value is not a valid enumeration member; permitted:' in response_json['detail'][0]['msg'])
        self.assertTrue('value is not a valid enumeration member; permitted:' in response_json['detail'][1]['msg'])
        self.assertTrue('value is not a valid enumeration member; permitted:' in response_json['detail'][2]['msg'])

    def test_api_fail_invalid_fields(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "test1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": True,
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "test1"
                                ],
                                "table_name": "string",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(1, len(response_json['detail']))
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition',
                                                             'fields',
                                                             0, 'column_description'])
        self.assertEqual(response_json['detail'][0]['msg'], 'str type expected')

    def test_api_fail_fields_subset_check(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "test1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": 'string',
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "something"
                                ],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "test1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition',
                                                             'partitioned_fields'])
        self.assertTrue('must be a subset of the "fields" field' in response_json['detail'][0]['msg'])

    def test_api_fail_no_ast(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "test1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "string",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "test1"
                                ],
                                "table_name": "string",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": []
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['msg'], 'ensure this value has at least 1 items')

    def test_api_fail_data_mapping_key_unique(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "string",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "string",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    },
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition', 'data_mappings'])
        self.assertEqual(response_json['detail'][0]['msg'], 'Data Mapping keys must be unique.')

    def test_api_fail_target_column_name_mismatch(self):
        response = client.post("/config/validate", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "string",
                    "group_name": "string",
                    "schedule_frequency": "string",
                    "schedule_days": "string",
                    "parallel_db_name": "string",
                    "base_path": "string",
                    "notebooks_to_execute": "string",
                    "cluster_name": "string",
                    "config_entries": [
                        {
                            "config_entry_group_key": "string",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "string",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [
                                    "TGT_COL1"
                                ],
                                "table_name": "string",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "string",
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL2",
                                                "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "dependencies": "",
                            "load_type": "full",
                            "load_synapse": True
                        }
                    ]
                }
            ]
        })

        self.assertEqual(response.status_code, 422)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(len(response_json['detail']), 1)
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 'config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition', 'data_mappings'])
        self.assertTrue('must be a subset of the table_definition "fields" field' in response_json['detail'][0]['msg'])
