from transform_generator.api.api import app
from fastapi.testclient import TestClient
from transform_generator.api.adapter import adapter


import json
import os
import unittest

from transform_generator.lib.config import load_config_directory

client = TestClient(app)


class TestApiGenerateSql(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Resources', 'positive_cases')

    @unittest.skip("Enable this test only when testing with the local digital core repo is desired.")
    def test_digital_core_data(self):
        """
        This loads data from the digital core folder as a stress test.
        It requires a local digital core repository at a location called cons_digital_Core
        """
        """  DIGITAL CORE STRESS TEST """
        schema_path = '../../../cons_digital_core/generator/schema'
        config_path = '../../../cons_digital_core/generator/config'
        mapping_path = '../../../cons_digital_core/generator/mapping'
        project_config_path_and_file = '../../../cons_digital_core/generator/project_config/project_config.csv'

        project_config_dto = adapter.dtos_from_csvs(
            project_config_path_and_file,
            config_path,
            mapping_path,
            schema_path
        )

        json_dict = project_config_dto.dict()
        response = client.post("/generate/sql", json=json_dict)
        self.assertEqual(response.status_code, 200)

        num_config_entries = 0
        for config_entry_group in project_config_dto.config_entry_groups:
            num_config_entries += len(config_entry_group.config_entries)
        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(num_config_entries, len(response_json))

    def test_generate_sql_from_config_folder(self):
        """
        This test builds a Project Config hierarchical DTO structure from CSVs located in test folders.
        It submits the ProjectConfigDTO to a validation service to test validation of configured CSVs.
        """
        """ Load basic test resources """
        schema_path = os.path.join(self.resources_folder, 'schema')
        config_path = os.path.join(self.resources_folder, 'config')
        mapping_path = os.path.join(self.resources_folder, 'mapping')
        project_config_path_and_file = os.path.join(self.resources_folder, 'project_config', 'project_config_test.csv')

        project_config_dto = adapter.dtos_from_csvs(
            project_config_path_and_file,
            config_path,
            mapping_path,
            schema_path
        )

        json_dict = project_config_dto.dict()
        response = client.post("/generate/sql", json=json_dict)
        self.assertEqual(response.status_code, 200)

        configs_by_config_filename = load_config_directory(config_path)
        # program types are not included in the response json
        num_config_entries = sum(len(configs) for config_filename, configs in configs_by_config_filename.items() if
                                 'program' not in config_filename)

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(num_config_entries, len(response_json))
        self.assertEqual(1, len(response_json[0]['data_mapping_responses']), 1)
        expected_query = 'SELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2\nFROM SRC_DB1.SRC_TBL1'
        self.assertEqual(expected_query, response_json[0]['data_mapping_responses'][0]['query'])

    def test_generate_sql_from_json_success(self):
        response = client.post("/generate/sql", json={
                "key": "string",
                "config_entry_groups": [
                    {
                        "key": "key1",
                        "group_name": "string",
                        "schedule_frequency": "",
                        "schedule_days": "",
                        "parallel_db_name": "string",
                        "base_path": "string",
                        "notebooks_to_execute": "string",
                        "cluster_name": "string",
                        "config_entries": [
                            {
                                "config_entry_group_key": "config_entry_1",
                                "target_type": "table",
                                "target_language": "databricks",
                                "table_definition": {
                                    "database_name": "DB1",
                                    "fields": [
                                        {
                                            "name": "TGT_COL1",
                                            "data_type": "string",
                                            "nullable": True,
                                            "column_description": "a description",
                                            "primary_key": True
                                        },
                                        {
                                            "name": "TGT_COL2",
                                            "data_type": "string",
                                            "nullable": True,
                                            "column_description": "a description 2",
                                            "primary_key": False
                                        },
                                        {
                                            "name": "TGT_COL3",
                                            "data_type": "string",
                                            "nullable": True,
                                            "column_description": "a description 3",
                                            "primary_key": False
                                        }
                                    ],
                                    "partitioned_fields": [],
                                    "table_name": "TABLE1",
                                    "table_business_name": "string",
                                    "table_description": "string",
                                    "data_mappings": [
                                        {
                                            "key": "mapping_key1a",
                                            "target_column_names": [
                                                "TGT_COL1",
                                                "TGT_COL2",
                                                "TGT_COL3"
                                            ],
                                            "transformation_expressions": [
                                                {
                                                    "target_column_name": "TGT_COL1",
                                                    "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                                                },
                                                {
                                                    "target_column_name": "TGT_COL2",
                                                    "expression": "SRC_TBL1.SRC_COL2 FROM SRC_DB1.SRC_TBL1"
                                                }
                                            ]
                                        },
                                        {
                                            "key": "mapping_key1b",
                                            "target_column_names": [
                                                "TGT_COL3"
                                            ],
                                            "transformation_expressions": [
                                                {
                                                    "target_column_name": "TGT_COL3",
                                                    "expression": "SRC_TBL1.SRC_COL3 FROM SRC_DB1.SRC_TBL1"
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "dependencies": "",
                                "load_type": "full",
                                "load_synapse": True
                            },
                            {
                                "config_entry_group_key": "config_entry_2",
                                "target_type": "table",
                                "target_language": "databricks",
                                "table_definition": {
                                    "database_name": "DB1",
                                    "fields": [
                                        {
                                            "name": "TGT_COL3",
                                            "data_type": "string",
                                            "nullable": True,
                                            "column_description": "a description",
                                            "primary_key": True
                                        }
                                    ],
                                    "partitioned_fields": [],
                                    "table_name": "TABLE2",
                                    "table_business_name": "string",
                                    "table_description": "string",
                                    "data_mappings": [
                                        {
                                            "key": "mapping_key2",
                                            "target_column_names": [
                                                "TGT_COL3"
                                            ],
                                            "transformation_expressions": [
                                                {
                                                    "target_column_name": "TGT_COL3",
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

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(2, len(response_json))
        self.assertEqual(2, len(response_json[0]['data_mapping_responses']))
        expected_query = 'SELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2,\n\tCAST(NULL AS string) AS TGT_COL3\nFROM SRC_DB1.SRC_TBL1'
        self.assertEqual(expected_query, response_json[0]['data_mapping_responses'][0]['query'])

    def test_generate_sql_from_json_bad_expression(self):
        """
        This test passes an invalid CASE expression to test expression validation.
        @return:
        """
        response = client.post("/generate/sql", json={
            "key": "string",
            "config_entry_groups": [
                {
                    "key": "key1",
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
                            "target_table": "DB1.TABLE1",
                            "target_type": "table",
                            "target_language": "databricks",
                            "table_definition": {
                                "database_name": "DB1",
                                "fields": [
                                    {
                                        "name": "TGT_COL1",
                                        "data_type": "string",
                                        "nullable": True,
                                        "column_description": "a description",
                                        "primary_key": True
                                    }
                                ],
                                "partitioned_fields": [],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": [
                                    {
                                        "key": "mapping_key1",
                                        "target_column_names": [
                                            "TGT_COL1"
                                        ],
                                        "transformation_expressions": [
                                            {
                                                "target_column_name": "TGT_COL1",
                                                "expression": "CASE INVALID"
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
        self.assertEqual(response_json['detail'][0]['msg'], '(["line 1:12 mismatched input \'<EOF>\' expecting '
                                                            'KW_WHEN"], \'target_column_name: [TGT_COL1]\')')

    def test_generate_sql_simple_from_json_success(self):
        response = client.post("/generate/sql-simple", json=[{
            "database_name": "DB1",
            "fields": [
                {
                    "name": "TGT_COL1",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description",
                    "primary_key": True
                },
                {
                    "name": "TGT_COL2",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description 2",
                    "primary_key": False
                },
                {
                    "name": "TGT_COL3",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description 3",
                    "primary_key": False
                }
            ],
            "partitioned_fields": [],
            "table_name": "TABLE1",
            "table_business_name": "string",
            "table_description": "string",
            "data_mappings": [
                {
                    "key": "mapping_key1a",
                    "target_column_names": [
                        "TGT_COL1",
                        "TGT_COL2",
                        "TGT_COL3"
                    ],
                    "transformation_expressions": [
                        {
                            "target_column_name": "TGT_COL1",
                            "expression": "SRC_TBL1.SRC_COL1 FROM SRC_DB1.SRC_TBL1"
                        },
                        {
                            "target_column_name": "TGT_COL2",
                            "expression": "SRC_TBL1.SRC_COL2 FROM SRC_DB1.SRC_TBL1"
                        }
                    ]
                },
                {
                    "key": "mapping_key1b",
                    "target_column_names": [
                        "TGT_COL3"
                    ],
                    "transformation_expressions": [
                        {
                            "target_column_name": "TGT_COL3",
                            "expression": "SRC_TBL1.SRC_COL3 FROM SRC_DB1.SRC_TBL1"
                        }
                    ]
                }
            ]
        }])
        self.assertEqual(response.status_code, 200)

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(1, len(response_json))
        self.assertEqual(2, len(response_json[0]['data_mapping_responses']))
        expected_query = 'SELECT\n\tSRC_TBL1.SRC_COL1 AS TGT_COL1,\n\tSRC_TBL1.SRC_COL2 AS TGT_COL2,\n\tCAST(NULL AS string) AS TGT_COL3\nFROM SRC_DB1.SRC_TBL1'
        self.assertEqual(expected_query, response_json[0]['data_mapping_responses'][0]['query'])

    def test_generate_sql_simple_from_json_bad_expression(self):
        response = client.post("/generate/sql-simple", json=[{
            "database_name": "DB1",
            "fields": [
                {
                    "name": "TGT_COL1",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description",
                    "primary_key": True
                }
            ],
            "partitioned_fields": [],
            "table_name": "TABLE1",
            "table_business_name": "string",
            "table_description": "string",
            "data_mappings": [
                {
                    "key": "mapping_key1",
                    "target_column_names": [
                        "TGT_COL1"
                    ],
                    "transformation_expressions": [
                        {
                            "target_column_name": "TGT_COL1",
                            "expression": "CASE INVALID"
                        }
                    ]
                }
            ]
        }])
        self.assertEqual(response.status_code, 422)

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(response_json['detail'][0]['msg'], '(["line 1:12 mismatched input \'<EOF>\' expecting '
                                                            'KW_WHEN"], \'target_column_name: [TGT_COL1]\')')

    def test_api_fail_no_data_mapping(self):
        response = client.post("/generate/sql", json={
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
                                        "primary_key": False
                                    }
                                ],
                                "partitioned_fields": [],
                                "table_name": "TABLE1",
                                "table_business_name": "string",
                                "table_description": "string",
                                "data_mappings": []
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
        self.assertEqual(response_json['detail'][0]['loc'], ['body','config_entry_groups',
                                                             0, 'config_entries',
                                                             0, 'table_definition', 'data_mappings'])
        self.assertEqual(response_json['detail'][0]['msg'], 'table_definitions must contain at least one data_mapping for SQL generation.')

    def test_generate_sql_simple_fail_no_data_mapping(self):
        response = client.post("/generate/sql-simple", json=[{
            "database_name": "DB1",
            "fields": [
                {
                    "name": "TGT_COL1",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description",
                    "primary_key": True
                },
                {
                    "name": "TGT_COL2",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description 2",
                    "primary_key": False
                },
                {
                    "name": "TGT_COL3",
                    "data_type": "string",
                    "nullable": True,
                    "column_description": "a description 3",
                    "primary_key": False
                }
            ],
            "partitioned_fields": [],
            "table_name": "TABLE1",
            "table_business_name": "string",
            "table_description": "string",
            "data_mappings": []
        }])

        response_json = json.loads(response.content.decode('utf-8'))
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response_json['detail'][0]['msg'],
                         'table_definitions must contain at least one data_mapping for SQL generation.')
        self.assertEqual(response_json['detail'][0]['loc'], ['body', 0, 'data_mappings'])

    def test_generate_sql_simple_realistic(self):
        response = client.post("/generate/sql-simple", json=[
            {
                "database_name": "SOURCE_DB",
                "fields": [
                    {
                        "name": "ROW_CD",
                        "data_type": "STRING",
                        "nullable": True,
                        "column_description": "Row Code",
                        "primary_key": False
                    },
                    {
                        "name": "SRC_SYS_CD",
                        "data_type": "STRING",
                        "nullable": False,
                        "column_description": "Source System Cd",
                        "primary_key": False
                    },
                    {
                        "name": "MATL_NUM",
                        "data_type": "STRING",
                        "nullable": False,
                        "column_description": "Mat Nbr",
                        "primary_key": False
                    },
                    {
                        "name": "BTCH_NUM",
                        "data_type": "STRING",
                        "nullable": True,
                        "column_description": "Batch",
                        "primary_key": False
                    },
                    {
                        "name": "PLNT_CD",
                        "data_type": "STRING",
                        "nullable": True,
                        "column_description": "Location Type Cd",
                        "primary_key": False
                    },
                    {
                        "name": "QA_RELEASE_START_DT",
                        "data_type": "TIMESTAMP",
                        "nullable": True,
                        "column_description": "QA Release Start Date",
                        "primary_key": False
                    },
                    {
                        "name": "DAI_BTCH_ID",
                        "data_type": "BIGINT",
                        "nullable": True,
                        "column_description": "DAI ETL Id",
                        "primary_key": False
                    },
                    {
                        "name": "DAI_UPDT_DTTM",
                        "data_type": "TIMESTAMP",
                        "nullable": True,
                        "column_description": "DAI Update Dttm",
                        "primary_key": False
                    },
                    {
                        "name": "DAI_CRT_DTTM",
                        "data_type": "TIMESTAMP",
                        "nullable": True,
                        "column_description": "DAI Create Dttm",
                        "primary_key": False
                    }
                ],
                "partitioned_fields": [],
                "table_name": "DEST_TABLE",
                "table_business_name": "Leadtime Table",
                "table_description": "Leadtime Table",
                "data_mappings": [
                    {
                        "key": "T_1643219409606",
                        "transformation_expressions": [
                            {
                                "target_column_name": "ROW_CD",
                                "expression": "CONCAT(ITEM.SRC_SYS_CD,ITEM.MATL_NUM,ITEM.BTCH_NUM,ITEM.PLNT_CD)"
                            },
                            {
                                "target_column_name": "SRC_SYS_CD",
                                "expression": "ITEM.SRC_SYS_CD"
                            },
                            {
                                "target_column_name": "MATL_NUM",
                                "expression": "ITEM.MATL_NUM"
                            },
                            {
                                "target_column_name": "BTCH_NUM",
                                "expression": "ITEM.BTCH_NUM"
                            },
                            {
                                "target_column_name": "PLNT_CD",
                                "expression": "ITEM.PLNT_CD"
                            },
                            {
                                "target_column_name": "QA_RELEASE_START_DT",
                                "expression": "MAX(ITEM.PSTNG_DT) FROM SC_CORE.ITEM JOIN SOURCE_DB.LOC ON (ITEM.SRC_SYS_CD = LOC.SRC_SYS_CD AND ITEM.MATL_NUM = LOC.MATL_NUM AND  ITEM.PLNT_CD = LOC.LOC_CD) JOIN SOURCE_DB.MATL  ON  (ITEM.SRC_SYS_CD = MATL.SRC_SYS_CD AND ITEM.MATL_NUM = MATL.MATL_NUM ) WHERE MATL.SRC_SYS_CD = '\''CAP'\'' AND ITEM.MATL_MVMT_TYPE_CD = '\''101'\'' AND MATL.MATL_TYPE_CD = '\''FERT'\'' AND LOC.MRP_TYPE_CD !=  '\''ND'\''  \nGROUP BY  \nITEM.SRC_SYS_CD, \nITEM.MATL_NUM,\nITEM.BTCH_NUM, ITEM.PLNT_CD, CONCAT(ITEM.SRC_SYS_CD,ITEM.MATL_NUM,ITEM.BTCH_NUM,ITEM.PLNT_CD)"
                            },
                            {
                                "target_column_name": "DAI_BTCH_ID",
                                "expression": "CAST(CURRENT_TIMESTAMP() AS BIGINT)"
                            },
                            {
                                "target_column_name": "DAI_UPDT_DTTM",
                                "expression": "CURRENT_TIMESTAMP()"
                            },
                            {
                                "target_column_name": "DAI_CRT_DTTM",
                                "expression": "CURRENT_TIMESTAMP()"
                            }
                        ]
                    }
                ]
            }
        ])
        self.assertEqual(200, response.status_code)