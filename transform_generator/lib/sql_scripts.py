from typing import Dict, List, Tuple

from transform_generator.generator.generator_error import GeneratorException
from transform_generator.lib.data_error import DataException
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.lib.logging import get_logger
from transform_generator.reader.table_definition_reader import get_table_definition

logger = get_logger(__name__)


def get_db_table_name(database_name: str, table_name: str, project_config: Dict[str, ProjectConfigEntry] = {},
                      config_filename_by_target_table: Dict[str, str] = {}) -> Tuple[str, str]:
    """
    This function takes in a database name and table name, looks up using the two dictionaries if the database name
    needs to be replaced with the variable format (for virtualization), and returns the updated database and table names
    @param database_name: A string of the database name
    @param table_name: A string of the table name
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filename_by_target_table: A dictionary with the target table as the key and the config filename as the
        values
    @return: A tuple containing the updated database name (replaced with a variable if it is virtualized) and the table
        name
    """
    database_table_name = database_name + '.' + table_name
    if project_config and database_table_name in config_filename_by_target_table:
        config_filename = config_filename_by_target_table[database_table_name]
        project_config_entry = project_config[config_filename]
        if project_config_entry.parallel_db_name != '':
            database_name = '${' + project_config_entry.parallel_db_name + '_db}'
    return database_name, table_name


def generate_script_header(config_value: ConfigEntry):
    script_header = ''

    logger.info(f'Generating Scripts for : {config_value.target_table}')

    for tuning_parameter in config_value.tuning_parameter:
        if '%run' in tuning_parameter:
            script_header += '\n-- COMMAND --\n\n' + tuning_parameter + '\n\n-- COMMAND --\n'
        elif tuning_parameter:
            script_header += tuning_parameter + ';\n\n'

    return script_header


def generate_script_insert(language: str, data_mapping: DataMapping, table_definition: TableDefinition,
                           select_query: str, first_query: bool, project_config: Dict[str, ProjectConfigEntry],
                           config_filename_by_target_table: Dict[str, str]) -> str:
    columns_not_in_table_def = data_mapping.target_column_names - table_definition.fields.keys()

    if columns_not_in_table_def:
        raise DataException('Columns [' + ','.join(columns_not_in_table_def) + '] are not present in table schema file')

    query_start = 'INSERT INTO TABLE '

    if language.lower() == 'hive':
        script_insert = query_start + data_mapping.database_name + '.' + data_mapping.table_name + " "

    elif language.lower() == 'impala':
        script_insert = query_start + data_mapping.database_name + '.' + data_mapping.table_name + ' (' + ','.join(
            table_definition.non_partitioned_fields.keys()) + ') '

    elif language.lower() == 'databricks':
        if first_query or len(table_definition.partitioned_fields) > 0:
            query_start = 'INSERT OVERWRITE '
        database_name, table_name = get_db_table_name(data_mapping.database_name, data_mapping.table_name,
                                                      project_config, config_filename_by_target_table)
        script_insert = query_start + database_name + '.' + table_name + '\n'

    else:
        raise DataException(('Unrecognized target language {}', language))

    if len(table_definition.partitioned_fields) > 0:
        script_insert += '\n' + 'PARTITION(' + ",".join(table_definition.partitioned_fields.keys()) + ')\n'
    script_insert += select_query + ";"

    return script_insert


def generate_script_merge(language: str, data_mapping: DataMapping, table_definition: TableDefinition,
                          select_query: str, first_query: bool, project_config: Dict[str, ProjectConfigEntry],
                          config_filename_by_target_table: Dict[str, str]) -> str:
    """
    This function takes in the following information and returns the merge query
    @param language: query language
    @param data_mapping: source to target mapping info
    @param table_definition: table in which to merge the data
    @param select_query: select query to extract the source data
    @param project_config: generator config file
    @param config_filename_by_target_table: used to get db name and table name
    @return: A merge query statement
    """
    columns_not_in_table_def = data_mapping.target_column_names - table_definition.fields.keys()

    if columns_not_in_table_def:
        raise DataException('Columns [' + ','.join(columns_not_in_table_def) + '] are not present in table schema file')

    query_start = 'MERGE INTO '

    if language.lower() == 'databricks':
        database_name, table_name = get_db_table_name(data_mapping.database_name, data_mapping.table_name,
                                                      project_config, config_filename_by_target_table)
        script_merge = query_start + database_name + '.' + table_name + '\n'

    else:
        raise DataException('Unrecognized target language {}', language)

    l0_table_with_schema = next(iter(data_mapping.table_dependencies), None)
    l0_table = l0_table_with_schema.split('.')[1]

    script_merge += 'USING ( ' + select_query + " )" + ' AS ' + l0_table

    script_merge += '\n' +  'ON '

    count_pk = 0
    for key in table_definition.primary_keys:
        if count_pk > 0:
            script_merge += 'AND '
        script_merge += table_name + '.' + key + ' = ' + l0_table + '.' + key + '\n'
        count_pk += 1

    script_merge += 'WHEN MATCHED THEN UPDATE SET'
    count = 0
    for key in table_definition.fields:
        if count > 0:
            script_merge += ','
        script_merge += '\n\t' + table_name + '.' + key + ' = ' + l0_table + '.' + key
        count += 1

    script_merge += '\n' + 'WHEN NOT MATCHED THEN INSERT ('
    count = 0
    for key in table_definition.fields:
        if count > 0:
            script_merge += ','
        script_merge += '\n\t' + key
        count += 1

    script_merge += '\n' + ') VALUES ('
    count = 0
    for key in table_definition.fields:
        if count > 0:
            script_merge += ','
        script_merge += '\n\t' + l0_table + '.' + key
        count += 1
    script_merge += '\n' + ')'

    return script_merge


def generate_update_audit(data_mapping: DataMapping):
    """
    This function generates a query to update the last extraction date for the incremental loads
    @param data_mapping: source to target mapping info to extract source table info
    @return: A merge query to insert or update the latest extraction date
    """
    l0_table_with_schema = next(iter(data_mapping.table_dependencies), None)

    query = """MERGE INTO  SC_P_CON_CORE.CDC_AUDIT
            USING (SELECT '""" + l0_table_with_schema + """' AS ENTITY) A
            ON CDC_AUDIT.ENTITY = A.ENTITY
            WHEN MATCHED THEN
            UPDATE SET  LAST_REFRESHED = CURRENT_DATE(),
            DAI_BTCH_ID = CURRENT_TIMESTAMP(),
            DAI_UPDT_DTTM = CAST(CURRENT_TIMESTAMP() AS BIGINT),
            DAI_CRT_DTTM = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED
            THEN INSERT (ENTITY, LAST_REFRESHED, DAI_BTCH_ID, DAI_UPDT_DTTM, DAI_CRT_DTTM) VALUES
            (A.ENTITY, CURRENT_DATE(), CURRENT_TIMESTAMP(), CAST(CURRENT_TIMESTAMP() AS BIGINT),CURRENT_TIMESTAMP());"""

    return query


def generate_delete_update(data_mapping: DataMapping, schema_path: str):
    """
    This function generates a query to delete necessary items from the specified database
    @param data_mapping: source to target mapping info to extract source table info
    @param schema_path: path to the directory containing schema files
    @return: the delete query
    """
    table_definition = get_table_definition(schema_path, data_mapping.database_name, data_mapping.table_name)

    primary_keys = table_definition.primary_keys

    and_statements = []
    for primary_key in primary_keys:
        pk_value = str(data_mapping.ast(primary_key).result_column)
        and_statements.append('AND target.' + primary_key + '=' + pk_value)
    and_statement_string = '\n'.join(and_statements)

    l0_table_with_schema = next(iter(data_mapping.table_dependencies), None)

    query = 'DELETE FROM ' + data_mapping.database_name + '.' + data_mapping.table_name + \
            '\nAS target WHERE exists (\nSELECT * FROM ' + l0_table_with_schema + '\nWHERE _deleted_=\'T\'\n' + \
            and_statement_string + ');'

    return query


def generate_script_cell(config_value, table_definition_value, queries_and_configs, data_mapping_value,
                         project_config, config_filename_by_target_table, schema_path):
    load_type = config_value.load_type
    language = config_value.target_language
    first_query = True
    if load_type != 'incremental' and len(table_definition_value.partitioned_fields) > 0:
        partitioned_query = '\n\nUNION ALL\n\n'.join(['--Below query is generated from : '
                                                      + mapping_filename + '\n' + query
                                                      for query, _, _, _, mapping_filename
                                                      in queries_and_configs])
        cell = generate_script_insert(language, data_mapping_value, table_definition_value,
                                      partitioned_query, first_query,
                                      project_config, config_filename_by_target_table)
        return [cell]
    else:
        cells = []
        for query, config_entry, data_mapping, table_definition, mapping_filename in queries_and_configs:
            cell = '-- Below query is generated from : ' + mapping_filename + '\n'
            if config_entry.load_type == 'incremental':
                cell += generate_script_merge(language, data_mapping, table_definition, query, first_query,
                                              project_config, config_filename_by_target_table)
                cell += ';\n\n' + generate_delete_update(data_mapping, schema_path)
                cell += '\n\n' + generate_update_audit(data_mapping)
            else:
                cell += generate_script_insert(language, data_mapping, table_definition, query, first_query,
                                               project_config, config_filename_by_target_table)
            first_query = False
            cells.append(cell)

        return cells


def generate_sql_scripts(queries_and_configs_by_target_table: Dict[str, List[Tuple[str, ConfigEntry]]],
                         project_config: Dict[str, ProjectConfigEntry],
                         config_filename_by_target_table: Dict[str, str], schema_path) -> Dict[str, List[str]]:
    script_cells_by_target_table = {}
    queries_config_items = [v[0] for v in queries_and_configs_by_target_table.values()]

    for select_query, config_value, data_mapping_value, table_definition_value, mapping_file in queries_config_items:
        
        target_table = data_mapping_value.database_name + "." + data_mapping_value.table_name

        cells = script_cells_by_target_table.get(target_table, [])
        script_header = generate_script_header(config_value)

        if script_header.strip():
            cells.append(script_header)

        if target_table not in script_cells_by_target_table:
            queries_and_configs = [v for v in queries_and_configs_by_target_table.get(data_mapping_value.table_name)]
            
            script_cells_by_target_table[target_table] = cells

            cells.extend(generate_script_cell(config_value, table_definition_value, queries_and_configs,
                                              data_mapping_value, project_config, config_filename_by_target_table,
                                              schema_path))
                    
    return script_cells_by_target_table
