import re
from os import path
import argparse
import json
from typing import Dict, List, Set, Tuple

from transform_generator.generator.generator_error import GeneratorException
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.requirement_failure_exception import RequirementFailureException
from transform_generator.generator.transform_gen import generate_select_query
from transform_generator.lib.config import get_config_structures
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.databricks import create_sql_widget
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.sql_scripts import get_db_table_name
from transform_generator.reader.table_definition_reader import read_table_definition
from transform_generator.lib.utils import write_file, escape_characters
from transform_generator.lib import config, data_mapping
from transform_generator.lib.datafactory.pipeline import rename_adf_activities, generate_ddl_datafactory_pipeline
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.lib.logging import get_logger
from transform_generator.DdlPathParameters import DdlPathParameters


logger = get_logger(__name__)


def _ddl_columns(table_definition: TableDefinition, data_type: bool = False, load_type: str = 'full'):
    """
    This function takes in a table definition and returns the part of the ddl call that lists out the column field names
    @param table_definition: TableDefinition generated based on schema file
    @param data_type: Boolean, true if the data type needs to be included in the output
    @return: A string of the column fields for the ddl output
    """
    column_strings = []
    if load_type == 'incremental':
        for field_name, field in table_definition.fields.items():
            column_string = '\t' + field_name
            if data_type and field.data_type.startswith('CHAR'):
                column_string += ' STRING ' 
            elif data_type and not field.data_type.startswith('CHAR'):  
                column_string += ' ' + field.data_type
            column_string += _sanitize_comment(field.column_description)
            column_strings.append(column_string)
    else:
        for field_name, field in table_definition.non_partitioned_fields.items():
            column_string = '\t' + field_name
            if data_type:
                column_string += ' ' + field.data_type
            column_string += _sanitize_comment(field.column_description)
            column_strings.append(column_string)
    columns = ',\n'.join(column_strings) + '\n)\n'
    return columns


def _partitioned_fields_ddl(table_definition: TableDefinition, load_type: str = 'full'):
    """
    This function takes in a table definition and returns the part of the ddl call that deals with partitioned fields
    @param table_definition: TableDefinition generated based on schema file
    @return: A string of the partitioned fields for the ddl output
    """
    partitioned_ddl = ''

    partitioned_fields = table_definition.partitioned_fields
    if len(partitioned_fields) > 0:
        partitioned_ddl += 'PARTITIONED BY \n(\n'
        partitions = []
        for field_name, field in partitioned_fields.items():
            if load_type == 'incremental':
                partitions.append('\t' + field_name)
            else:
                partitions.append('\t' + field_name + ' ' + field.data_type + ' COMMENT \'' + field.column_description +
                                  "'")
        partitions_statements = ',\n'.join(partitions)
        partitioned_ddl += partitions_statements + '\n)\n'
    return partitioned_ddl


def _sanitize_comment(comment: str):
    """
    This comment standardizes comment strings
    @param comment: String of the comment
    @return: An updated comment with the appropriate length of characters and formatting
    """
    if comment == '':
        return comment
    # Note: 256-character limit set here
    sanitized_comment = escape_characters(comment[0:255])
    sanitized_comment = " COMMENT '" + sanitized_comment + "'"
    return sanitized_comment


def _create_ddl(create_statement: str, drop_statement: str, table_definition: TableDefinition, table: bool,
                database_name: str, table_name: str, load_type: str = 'full') -> str:
    """
    This function creates the standard start of a ddl output file
    @param create_statement: String of what the create statement should start with
    @param drop_statement: String of what the drop statement should start with
    @param table_definition: TableDefinition generated based on schema file
    @param table: Boolean, true if we are creating a ddl for a table, false if it is for a view
    @return: string of the start of the ddl query
    """
    if load_type != 'incremental':
        ddl = drop_statement + database_name + '.' + table_name + ';\n\n'
        ddl += create_statement + database_name + '.' + table_name + '\n(\n'
    else:
        ddl = create_statement + 'IF NOT EXISTS ' + database_name + '.' + table_name + '\n(\n'
    ddl += _ddl_columns(table_definition, table, load_type)
    if load_type == 'incremental':
        ddl += 'USING DELTA' + '\n'
    ddl += _partitioned_fields_ddl(table_definition, load_type)
    ddl += _sanitize_comment(table_definition.table_description).strip() + '\n'
    return ddl


def create_ddl_view(target_table: str, load_type: str, target_language: str, input_files: List[str],
                    mappings_by_mapping_filename: Dict[str, data_mapping.DataMapping],
                    table_definition: TableDefinition, project_config: Dict[str, ProjectConfigEntry],
                    config_filename_by_target_table: Dict[str, str]) -> str:
    """
    function that generates a ddl create view statement
    @param target_table: string of the database_name.table_name combo
    @param load_type: string of the load type, defaults to full, incremental is another used option
    @param target_language: string of target language, defaults to databricks
    @param input_files: list of strings of the input files for a table
    @param mappings_by_mapping_filename: mappings for view option
    @param table_definition: table definition generated based on schema file
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filename_by_target_table: A dictionary with the target table as the key and the config filename as the
        values
    @return: string of a ddl query, updated dictionary of table dependencies for the views,
        and the target table that the config_row references
    """
    database_name, view_name = target_table.split('.')
    database_name, view_name = get_db_table_name(database_name, view_name, project_config,
                                                 config_filename_by_target_table)
    ddl = _create_ddl('CREATE VIEW ', 'DROP VIEW IF EXISTS ', table_definition, False, database_name, view_name,
                      load_type)

    if not input_files:
        raise RequirementFailureException('At least one mapping file is required to create a view')
    ddl += ' AS '
    select_statements = []
    for mapping_filename in input_files:
        data_mappings = mappings_by_mapping_filename[mapping_filename]
        select_statements.append('( ' + generate_select_query(data_mappings, table_definition, target_language,
                                                              project_config, config_filename_by_target_table,
                                                              load_type) + ' )\n')
    ddl += ' UNION ALL '.join(select_statements) + ';'

    return ddl


def generate_ddl_statements_from_configs(
        configs: List[ConfigEntry], full_config_file_name: str,
        mappings_by_mapping_filename: Dict[str, data_mapping.DataMapping],
        schema_path: str, project_config: Dict[str, ProjectConfigEntry],
        config_filename_by_target_table: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """
    function that generates the ddl create table/view statements for a config file
    @param configs: a list of config entries contained in a configuration file
    @param full_config_file_name: string of the name of the configuration file
    @param mappings_by_mapping_filename: dictionary of mappings for view option
    @param schema_path: string path of the schema files
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filename_by_target_table: A dictionary with the target table as the key and the config filename as the
        values
    @return: dictionary of ddl dependencies and dictionary of config files by target tables
    """
    try:
        queries = []
        views = []

        for row in configs:
            target_table = row.target_table.upper()
            schema_file = target_table + '.csv'
            full_schema_path = path.join(schema_path, schema_file)

            if path.exists(full_schema_path):
                logger.info('Generating Scripts for : ' + row.target_table)
                table_definition = read_table_definition(path.join(schema_path, target_table))
                ddl = generate_ddl_statements(table_definition, target_table, row.target_type, row.load_type,
                                              row.target_language, row.input_files, config_filename_by_target_table,
                                              full_config_file_name, project_config, mappings_by_mapping_filename,)
                if ddl:
                    views.append(ddl) if row.target_type == 'view' else queries.append(ddl)

            else:
                logger.error('schema file for (%s) not found!', schema_file)

        return queries, views

    except GeneratorException as ex:
        for error in ex.errors:
            logger.error(error)


def generate_ddl_statements(table_definition, target_table, target_type: str = 'table', load_type: str = 'full',
                            target_language: str = 'databricks', input_files: List[str] = [],
                            config_filename_by_target_table: Dict[str, str] = {}, full_config_file_name: str = '',
                            project_config:  Dict[str, ProjectConfigEntry] = {},
                            mappings_by_mapping_filename: Dict[str, data_mapping.DataMapping] = {}) -> str:
    """
    Function to create one ddl statement
    @param table_definition: table definition generated based on schema file
    @param target_table: string of the database_name.table_name combo
    @param target_type: string of type to create, defaults to table, view is another used option
    @param load_type: string of the load type, defaults to full, incremental is another used option
    @param target_language: string of target language, defaults to databricks
    @param input_files: list of strings of the input files for a table
    @param config_filename_by_target_table: Dictionary with target table as the key and config filename as the value
    @param full_config_file_name: string of the complete config file name
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param mappings_by_mapping_filename: A dictionary with a key of mapping filename and value of the DataMapping
    @return: a string of the ddl create view or create table statement
    """
    if target_type == 'view':
        ddl = create_ddl_view(target_table, load_type, target_language, input_files,
                              mappings_by_mapping_filename, table_definition, project_config,
                              config_filename_by_target_table)
    elif target_type == 'lineage':
        ddl = ''
    else:
        ddl = create_ddl_table(table_definition, target_table, load_type, target_language,
                               project_config, config_filename_by_target_table, full_config_file_name)
    return ddl


def write_files(queries: List[str], views: List[str], notebooks_to_execute: str, base_path_widget_text: str,
                output_file_path: str, widget_text):

    """
    @param output_file_path: string path of where the output files go

    """

    if notebooks_to_execute:
        notebooks_to_execute = '%run\n' + '\n'.join(notebooks_to_execute.split(','))

    cell_separation_comment = "\n\n-- COMMAND --\n\n"

    if len(queries) > 0:
        sql_text = cell_separation_comment.join(queries)
        if widget_text:
            sql_text = cell_separation_comment.join([widget_text, sql_text])
        if notebooks_to_execute:
            sql_text = cell_separation_comment.join([notebooks_to_execute, sql_text])
        if base_path_widget_text:
            sql_text = cell_separation_comment.join([base_path_widget_text, sql_text])

        output_file = output_file_path + '.sql'
        write_file(output_file, sql_text)
        logger.info('\tOutput file %s', output_file)

    if len(views) > 0:
        sql_text = cell_separation_comment.join(views)
        if widget_text:
            sql_text = cell_separation_comment.join([widget_text, sql_text])
        if notebooks_to_execute:
            sql_text = cell_separation_comment.join([notebooks_to_execute, sql_text])

        write_file(output_file_path + '_views.sql', sql_text)


def _create_base_path_widgets(base_path: str) -> str:
    """
    Takes in the custom base path string, extracts variables from it, and creates corresponding widgets.
    @param base_path: String of the custom base path from the generator config file.
    @return: A string of all the widgets of the variables from the generator config base path, empty string if there are
        no variables in the base path.
    """
    base_path_widget_text = ''
    if base_path:
        raw_base_path_parameters = re.findall(r'\${[^}]*}', base_path)
        base_path_widgets = []
        for parameter in raw_base_path_parameters:
            param = parameter.replace('${', '').replace('}', '')
            widget = 'CREATE WIDGET TEXT ' + param.split('.')[0] + ' DEFAULT \'\';'
            base_path_widgets.append(widget)
        base_path_widget_text = '\n'.join(base_path_widgets)
    return base_path_widget_text


def create_ddl_table(table_definition: TableDefinition, target_table: str, load_type: str = 'full',
                     target_language: str = 'databricks', project_config: Dict[str, ProjectConfigEntry] = {},
                     config_filename_by_target_table: Dict[str, str] = {}, config_filename: str = '') -> str:
    """
    function that generates a ddl create table statement
    @param table_definition: table definition generated based on schema file
    @param target_table: string of the database_name.table_name combo
    @param load_type: string of the load type, defaults to full, incremental is another used option
    @param target_language: string of target language, defaults to databricks
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filename_by_target_table: Dictionary with target table as the key and config filename as the value
    @param config_filename: String of the config filename the config row is a part of
    @return: string of a ddl query and the target table that the config_row references
    """

    database_name, table_name = target_table.split('.')
    database_name, table_name = get_db_table_name(database_name, table_name, project_config,
                                                  config_filename_by_target_table)
    ddl = _create_ddl('CREATE TABLE ', 'DROP TABLE IF EXISTS ', table_definition, True, database_name, table_name,
                      load_type)

    if load_type != 'incremental':
        ddl += 'STORED AS PARQUET'

    if target_language.lower() == 'databricks':
        if project_config and config_filename in project_config and project_config[config_filename].base_path:
            ddl += f'\nLOCATION \'{project_config[config_filename].base_path}{database_name.lower()}/{table_name.lower()}\''
        else:
            ddl += f'\nLOCATION \'/mnt/dct/chdp/{database_name.lower()}/{table_name.lower()}\''

        if load_type != 'incremental' and len(table_definition.partitioned_fields) > 0:
            ddl += f';\n\nALTER TABLE {database_name}.{table_name} RECOVER PARTITIONS'

    ddl += ';'

    return ddl


def get_target_types(configs: List[ConfigEntry]) -> Set[str]:
    """
    Creates a set of the target_types contained in a transformation config file
    @param configs: a List of config entries
    @return: A set of strings of target_types in each of the config entries
    """
    target_types = set()
    for config_entry in configs:
        target_types.add(config_entry.target_type)
    return target_types


def get_module_name(project_config: Dict[str, ProjectConfigEntry], config_filenames: Set[str]) -> Tuple[bool, str]:
    """
    Extract if single module and the single module name
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filenames: A set containing the config filenames
    @return: A tuple containing a boolean (true if it is a single module) and a string of the unique module name (if
        only one is being run) or an empty string if there is more than one module being run
    """
    modules = set()
    for config_filename in config_filenames:
        modules.add(project_config[config_filename].group_name)
    if len(modules) == 1:
        return True, ''.join(modules)
    else:
        return False, ''


def generate_ddl_output(ddl_paths: DdlPathParameters, database_param_prefix: str = ''):
    """
    main function to create output ddl files and pipelines
    :param ddl_paths: Object containing all the necessary paths to generate the ddls
    :param  database_param_prefix: A string containing the prefix for the ADF Pipelines if any database parameterization
        flags are Y
    """
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_config_structures(ddl_paths.config_path,
                                                                             ddl_paths.mapping_path,
                                                                             database_param_prefix,
                                                                             ddl_paths.project_config_paths,
                                                                             ddl_paths.external_module_config_paths)

    single_module, module_name = get_module_name(project_config, set(configs_by_config_filename.keys()))

    widget_text = create_sql_widget(project_config)

    adf_activity_names = set()
    table_dependencies_by_view = {}
    tables_in_ddl = {}

    for full_config_file_name, configs in configs_by_config_filename.items():
        queries, views = generate_ddl_statements_from_configs(configs, full_config_file_name,
                                                              mappings_by_mapping_filename, ddl_paths.schema_path,
                                                              project_config, config_filename_by_target_table)

        table_dependencies_by_view, tables_in_ddl = get_view_table_dependencies(tables_in_ddl,
                                                                                table_dependencies_by_view, configs,
                                                                                full_config_file_name,
                                                                                mappings_by_mapping_filename)

        base_path_widget_text = _create_base_path_widgets(project_config[full_config_file_name].base_path)
        output_file_path = path.join(ddl_paths.output_dir_databricks, 'databricks', 'generated', 'ddl',
                                     full_config_file_name.replace('.csv', ''))
        write_files(queries, views, project_config[full_config_file_name].notebooks_to_execute, base_path_widget_text,
                    output_file_path, widget_text)

        platform = config.get_platform_for_configs(configs)
        if platform == 'databricks' and (queries or views):
            adf_activity_names.add(full_config_file_name.replace('.csv', ''))

    ddl_file_deps = {}
    for view, tables in table_dependencies_by_view.items():
        adf_activity_names.add(view)
        ddl_files = set()
        for table in tables:
            if table.lower() in tables_in_ddl:
                ddl_files.add(rename_adf_activities(tables_in_ddl[table.lower()]))
        ddl_file_deps[view] = sorted(ddl_files)

    if adf_activity_names:
        adf_activity_names = sorted(adf_activity_names)
        adf_file_name = 'ddl'
        if single_module and module_name != '':
            adf_file_name = module_name + '_' + adf_file_name

        if database_variables:
            adf_file_name = database_param_prefix + '_' + adf_file_name
        folder_path = '/databricks/generated/ddl'

        adf_pipeline = generate_ddl_datafactory_pipeline(adf_activity_names, adf_file_name, ddl_file_deps,
                                                         ddl_paths.target_notebook_folder, folder_path,
                                                         database_variables, project_config, database_param_prefix)
        pipeline_output_file_path = path.join(ddl_paths.output_dir_datafactory, 'datafactory', 'pipelines', 'generated',
                                              'DDL', adf_file_name + '.json')
        write_file(pipeline_output_file_path, json.dumps(adf_pipeline, indent=4))
        logger.info('\tPipeline output file %s', pipeline_output_file_path)


def get_view_table_dependencies(tables_in_ddl: Dict[str, str], table_dependencies_by_view: Dict[str, List[str]],
                                configs: List[ConfigEntry], full_config_file_name: str,
                                mappings_by_mapping_filename: Dict[str, DataMapping]) -> Tuple[Dict[str, List[str]],
                                                                                               Dict[str, str]]:
    """
    Determines what tables a view is dependent on
    @param tables_in_ddl: A dictionary of all the tables with the value as the config file they are found in
    @param table_dependencies_by_view: A dictionary with the key being the name of the config file view and a list of
        tables it depends on
    @param configs: A list of config entries
    @param full_config_file_name: the full name of the config file
    @param mappings_by_mapping_filename: A dictionary with a key of mapping filename and value of the DataMapping
    @return: updated table_dependencies_by_view and tables_in_ddl
    """
    config_file_name = full_config_file_name.replace('.csv', '')
    for config_row in configs:
        if config_row.target_type == 'view':
            config_file_name_view = config_file_name + '_views'
            table_dependencies = set(table_dependencies_by_view.get(config_file_name_view, {}))
            for mapping_filename in config_row.input_files:
                data_mappings = mappings_by_mapping_filename[mapping_filename]
                table_dependencies |= data_mappings.table_dependencies
            table_dependencies_by_view[config_file_name_view] = sorted(table_dependencies)
        tables_in_ddl[config_row.target_table.lower()] = config_file_name
    return table_dependencies_by_view, tables_in_ddl


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process data mapping files.')
    parser.add_argument('--config_path', help='Path to config directory')
    parser.add_argument('--schema_path', help='Path to directory containing CSV Schema files')
    parser.add_argument('--mapping_sheet_path', help='Path to base directory containing data mapping sheets')
    parser.add_argument('--output_databricks', help='Base output directory for SQL files and Python scripts')
    parser.add_argument('--output_datafactory', help='Base output directory for ADF pipelines')
    parser.add_argument('--databricks_folder_location', help='Databricks folder location')
    parser.add_argument('--database_param_prefix', default='',
                        help='String prefix for the ADF Pipelines where parameterization')
    parser.add_argument('--project_config_paths',
                        help='Semicolon (;) delimited string of paths to project config files')
    parser.add_argument('--external_module_config_paths',
                        help='Semicolon (;) delimited string of paths to external module config directory paths')

    args = parser.parse_args()

    paths_data_object = DdlPathParameters(args.config_path, args.schema_path, args.mapping_sheet_path,
                                          args.project_config_paths, args.output_databricks, args.output_datafactory,
                                          args.databricks_folder_location, args.external_module_config_paths)

    generate_ddl_output(paths_data_object, args.database_param_prefix)
