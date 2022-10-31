import csv
import json
from collections import OrderedDict
from os import path

from transform_generator.generator.field import Field
from transform_generator.lib.logging import get_logger
from transform_generator.lib.table_definition import TableDefinition

logger = get_logger(__name__)


def read_table_definition(schema_file_path):
    if path.exists(schema_file_path + ".json"):
        return read_table_definition_json(schema_file_path)
    else:
        return read_table_definition_csv(schema_file_path)


def read_table_definition_json(schema_file_path, db_name=None):
    logger.info("Loading json table definition: %s", schema_file_path)
    fields = OrderedDict()
    partitions = {}
    primary_keys = []
    _file_extension = "json"
    schema_file_path = schema_file_path + "." + _file_extension
    f = open(schema_file_path, newline='')
    json_parameterss = len(f.readlines())
    if json_parameterss <= 1:
        f.close()
        raise ValueError("Empty file : " + schema_file_path)
    f.close()

    with open(schema_file_path, newline='') as json_file:
        json_parameters = json.load(json_file)
        table_name = str(json_parameters['entity']).strip()
        table_business_name = str(json_parameters["entity_name"]).strip()
        table_description = json_parameters.get('table_business_desc', '')
        attributes = json_parameters["attributes"]

        for attribute in attributes:
            name = str(attribute["id"]).strip()
            data_type = str(attribute["data_type"]).strip().upper()
            nullable = str(attribute["nullable"]).strip()
            column_description = str(attribute["desc"])
            partition_key = attribute.get("partition_key", '')

            if (partition_key != '') and (partition_key == 'True'):
                partition_datatype = data_type
                partition_comment = attribute.get("partition_comment", '')
                partitions[partition_key] = (partition_key, partition_datatype, partition_comment)

            if attribute.get("prime_key", 'False') == 'True':
                primary_keys.append(name)

            if data_type.startswith("VARCHAR"):
                data_type = "STRING"

            fields[name] = Field(name, data_type, nullable, column_description, 0, 0)

    json_file.close()
    return table_definition(db_name, table_name, table_business_name, table_description, fields, partitions,
                            primary_keys)


def read_table_definition_csv(schema_file_path, db_name=None):
    logger.info("Loading csv table definition: %s", schema_file_path)
    fields = OrderedDict()
    partitions = {}
    primary_keys = []
    _file_extension = "csv"
    schema_file_path = schema_file_path + "." + _file_extension

    f = open(schema_file_path, encoding="utf-8-sig", mode='r')
    rows = len(f.readlines())
    if rows <= 1:
        f.close()
        raise ValueError("Empty file : " + schema_file_path)
    f.close()
    with open(schema_file_path, encoding="utf-8-sig", mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            table_name = str(row["TABLE NAME"]).strip()
            table_business_name = str(row["TABLE BUSINESS NAME"]).strip()
            name = str(row["COLUMN NAME"]).strip()
            data_type = str(row["COLUMN DATA TYPE"]).strip().upper()
            nullable = str(row["COLUMN NULLABLE"]).strip()
            column_description = str(row["COLUMN BUSINESS DESC"])
            partition_key = row.get("PARTITION KEY", '')
            if (partition_key != '') and (partition_key != 'N'):
                if partition_key == 'Y':
                    partition_key = name
                partition_datatype = row.get("PARTITION DATA TYPE", '')
                partition_comment = row.get("PARTITION COMMENT", '')
                partitions[partition_key] = (partition_key, partition_datatype, partition_comment)
            table_description = str(row["TABLE BUSINESS DESC"])

            if row.get("PRIMARY KEY", 'N') == 'Y':
                primary_keys.append(name)

            if data_type.startswith("VARCHAR"):
                data_type = "STRING"

            fields[name] = Field(name, data_type, nullable, column_description, 0, 0)

    csv_file.close()
    return table_definition(db_name, table_name, table_business_name, table_description, fields, partitions,
                            primary_keys)


def table_definition(db_name: str, table_name: str, table_business_name: str, table_description: str, fields, partitions,
                     primary_keys):
    partitioned_fields = OrderedDict()
    counter = 1
    while counter <= len(partitions):
        for partition_key, partition_datatype, partition_comment in partitions.values():
            if partition_comment is not None and partition_comment[0].isdigit() and int(
                    partition_comment[0]) == counter:
                partitioned_fields[partition_key] = fields[partition_key]
            elif partition_comment is not None and not partition_comment[0].isdigit() and counter == 1:
                partitioned_fields[partition_key] = fields[partition_key]
        counter += 1

    return TableDefinition(db_name, table_name, table_business_name, table_description, fields, partitioned_fields,
                           primary_keys)


table_definitions_by_table_name = {}


def get_table_definition(schema_path: str, db: str, tbl: str) -> TableDefinition:
    """
    Returns the TableDefinition for the specified database and table.
    The TableDefinition object will be loaded from a file at the path specified by 'schema_path.' Table definitions
    are cached in a dictionary, and the cached object will be returned if found.
    :param schema_path: The path where the schema files can be found
    :param db: The name of the database for the table definition.
    :param tbl: The name of the table for the table definition.
    :return: A TableDefinition object containing the specified table definition.
    """
    db_tbl_name = db.upper() + "." + tbl.upper()
    if db_tbl_name not in table_definitions_by_table_name:
        table_def = read_table_definition(path.join(schema_path, db_tbl_name))
        table_definitions_by_table_name[db_tbl_name] = table_def
        return table_def
    else:
        return table_definitions_by_table_name[db_tbl_name]
