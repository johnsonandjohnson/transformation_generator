import argparse
import json
from os import path, makedirs
from typing import Dict, List, Tuple

from transform_generator.generator.transform_gen import generate_select_query
from transform_generator.lib.config import get_config_structures
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.databricks import create_sql_widget
from transform_generator.lib.datafactory.pipeline import generate_datafactory_pipeline, \
    get_notebook_paths_by_target_table, \
    rename_adf_activities, generate_module_adf_pipeline
from transform_generator.lib.dependency_analysis import get_table_dependency_edges_from_configs, build_dependency_graph
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.logging import get_logger
from transform_generator.lib.sql_scripts import generate_sql_scripts, get_db_table_name
from transform_generator.reader.table_definition_reader import get_table_definition
from transform_generator.PathParameters import PathParameters
from project_group import load_project_group

logger = get_logger(__name__)


def write_file(output_file, query):
    makedirs(path.dirname(output_file), exist_ok=True)
    f = open(output_file, "w")
    f.write(query)
    f.close()


def generate_select_queries(configs: List[ConfigEntry], mappings_by_mapping_filename: Dict[str, DataMapping],
                            schema_path: str, project_config: Dict[str, ProjectConfigEntry],
                            config_filename_by_target_table: Dict[str, str]) -> Dict[
        str, List[Tuple[str, ConfigEntry]]]:
    """
    Generate all SQL queries and return a dictionary with a key of target table and a value a list of tuples containing
    the transformation queries and the corresponding config entry used to create them.
    @param configs: A list of config entries to process
    @param mappings_by_mapping_filename: A dictionary with a key of mapping filename and value of the DataMapping
    @param schema_path: A path where table definition files can be found.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param config_filename_by_target_table: A dictionary with the target table as the key and the config filename as the
        values
    @return: A dictionary with a key of target table and a value of a list tuples containing the transformation query
        and the ConfigEntry, DataMapping, and TableDefinition used to generate it.
    """
    queries_and_configs_by_target_table = {}

    for config_entry in configs:
        target_type = config_entry.target_type.lower() if config_entry.target_type else ''
        if target_type not in {'view', 'program', 'lineage'}:
            for mapping_filename in config_entry.input_files:
                data_mapping = mappings_by_mapping_filename[mapping_filename]
                table_definition = get_table_definition(schema_path, data_mapping.database_name,
                                                        data_mapping.table_name)

                select_query = generate_select_query(data_mapping, table_definition, config_entry.target_language,
                                                     project_config, config_filename_by_target_table,
                                                     config_entry.load_type)

                queries_and_configs = queries_and_configs_by_target_table.get(data_mapping.table_name, [])
                queries_and_configs.append(
                    (select_query, config_entry, data_mapping, table_definition, mapping_filename))
                queries_and_configs_by_target_table[data_mapping.table_name] = queries_and_configs
    return queries_and_configs_by_target_table


def write_queries_to_files_azure(script_cells_by_target_table: Dict[str, List[str]], output_path: str, widget_text):
    """
    Write transformation queries to DataBricks notebooks.
    For azure, we write all queries related to a single target table into a single notebook. Please note, the order
    of the queries may matter - Some queries will "overwrite" the target table to clear data.
    """
    filenames_by_target_table = {}
    for target_table, cells in script_cells_by_target_table.items():
        filename = target_table + '.sql'
        output_file_path = path.join(output_path, 'databricks', 'generated', 'dml', filename)
        sql_text = '\n\n-- COMMAND --\n\n'.join(cells)
        if widget_text:
            sql_text = '\n\n-- COMMAND --\n\n'.join([widget_text, sql_text])

        write_file(output_file_path, sql_text)
        filenames_by_target_table[target_table] = filename
    return filenames_by_target_table


def generate_sql_output(config_path: str, schema_path: str, mapping_sheet_path: str, output_databricks: str,
                        output_datafactory: str, project_config_paths: str, folder_location: str,
                        database_param_prefix: str = '', external_module_config_paths: str = ''):
    """
    main function to create output sql files and pipelines
    @param config_path: string path to config directory
    @param schema_path: string path to directory containing CSV Schema files
    @param mapping_sheet_path: string path to base directory containing data mapping sheets
    @param output_databricks: string path to base output directory for SQL files and Python scripts
    @param output_datafactory: string path to base output directory for ADF Pipelines
    @param project_config_paths: A semicolon delimited string of paths to the project config files
    @param folder_location: string path to databricks folder location
    @param database_param_prefix: A string containing the prefix for the ADF Pipelines if any database parameterization
        flags are Y
    @param external_module_config_paths: A string of semicolon (;) delimited paths to external module config directories
    """
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_sheet_path,
                                                                             database_param_prefix,
                                                                             project_config_paths,
                                                                             external_module_config_paths)

    modules = set()
    widget_text = create_sql_widget(project_config)
    for config_file_name, configs in configs_by_config_filename.items():
        group_name = project_config[config_file_name].group_name
        modules.add(group_name)
        queries_and_configs_by_target_table = generate_select_queries(configs, mappings_by_mapping_filename,
                                                                      schema_path, project_config,
                                                                      config_filename_by_target_table)

        script_cells_by_target_table = generate_sql_scripts(queries_and_configs_by_target_table, project_config,
                                                            config_filename_by_target_table, schema_path)

        folder_path = ''
        if group_name != '':
            folder_path += '/' + group_name

        for target_table, cells in script_cells_by_target_table.items():
            notebooks_to_execute = project_config[config_filename_by_target_table[target_table]].notebooks_to_execute
            if notebooks_to_execute:
                notebooks_to_execute = '%run\n' + '\n'.join(notebooks_to_execute.split(','))
                cells.insert(0, notebooks_to_execute)
            config_entry = queries_and_configs_by_target_table[target_table.split('.')[1]][0][1]
            if config_entry.load_synapse:
                database_name, table_name = get_db_table_name(
                    target_table.split('.')[0],
                    target_table.split('.')[1],
                    project_config,
                    config_filename_by_target_table)
                database_name = database_name.translate(str.maketrans({'{': '', '}': ''}))
                if not database_variables:
                    database_name = '"' + database_name + '"'

                synapse_execution = f'%run {folder_location}/cons_digital_core/databricks/synapse/syn_load ' \
                                    f'$src_table = "{table_name}" $tgt_table = "{table_name}" $tgt_schema = ' \
                                    f'{database_name}'
                cells.append(synapse_execution)

        filenames_by_target_table = write_queries_to_files_azure(script_cells_by_target_table, output_databricks,
                                                                 widget_text)

        folder_path += '/databricks/generated/dml'

        notebook_paths_by_target_table = get_notebook_paths_by_target_table(
            configs_by_config_filename[config_file_name], filenames_by_target_table,
            folder_location, folder_path, database_variables)

        sequencing = {config_entry.target_table: config_entry.sequence for config_entry in configs if
                      config_entry.sequence}

        # Compute dependency graph just for this config file (workstream)
        dependency_edges = get_table_dependency_edges_from_configs(configs, mappings_by_mapping_filename)
        dependency_graph = build_dependency_graph(dependency_edges, sequencing)

        cluster_name = project_config[config_file_name].cluster_name
        if not cluster_name:
            cluster_name = 'HighConCluster'

        if dependency_graph:
            if database_variables:
                pipeline = generate_datafactory_pipeline(dependency_graph, notebook_paths_by_target_table,
                                                         config_file_name, project_config, cluster_name,
                                                         database_param_prefix)
            else:
                pipeline = generate_datafactory_pipeline(dependency_graph, notebook_paths_by_target_table,
                                                         config_file_name, project_config, cluster_name)

            if pipeline:
                output_adf_dir = path.join(output_datafactory, 'datafactory', 'pipelines', 'generated')
                adf_file_name = rename_adf_activities(config_file_name).replace('config_', '').replace('-csv', '.json')
                if "core" in adf_file_name:
                    output_adf_dir = path.join(output_adf_dir, 'CorePipelines')
                else:
                    output_adf_dir = path.join(output_adf_dir, 'SemanticPipelines')
                output_adf_file_path = path.join(output_adf_dir, adf_file_name)

                if not path.exists(output_adf_dir):
                    makedirs(output_adf_dir)

                # Save pipeline json
                with open(output_adf_file_path, 'w') as outfile:
                    json.dump(pipeline, outfile, indent=4)

    if len(modules) == 1:
        # Create module-level ADF Pipeline
        main_pipeline, module_pipeline_path = generate_module_adf_pipeline(config_filename_by_target_table,
                                                                           configs_by_config_filename,
                                                                           database_param_prefix, database_variables,
                                                                           project_config, group_name,
                                                                           mappings_by_mapping_filename,
                                                                           output_datafactory, modules)
        with open(module_pipeline_path, 'w') as outfile:
            json.dump(main_pipeline, outfile, indent=4)
        logger.info('\tPipeline output file %s', module_pipeline_path)
    else:
        logger.info('\tPipeline not created, %s contains config files with more than one module.', config_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process data mapping files.')
    parser.add_argument('--config_path', help='Path to config directory')
    parser.add_argument('--schema_path', help='Path to directory containing CSV Schema files')
    parser.add_argument('--mapping_sheet_path', help='Path to base directory containing data mapping sheets')
    parser.add_argument('--output_databricks', help='Base output directory for SQL files and Bash scripts')
    parser.add_argument('--output_datafactory', help='Base output directory for SQL files and Bash scripts')
    parser.add_argument('--databricks_folder_location', help='Databricks folder location')
    parser.add_argument('--database_param_prefix', default='',
                        help='String prefix for the ADF Pipelines where parameterization')
    parser.add_argument('--project_config_paths',
                        help='Semicolon (;) delimited string of paths to project config files')
    parser.add_argument('--external_module_config_paths',
                        help='Semicolon (;) delimited string of paths to external module config directory paths')

    args = parser.parse_args()

    generate_sql_output(args.config_path, args.schema_path, args.mapping_sheet_path, args.output_databricks,
                        args.output_datafactory, args.project_config_paths, args.databricks_folder_location,
                        args.database_param_prefix, args.external_module_config_paths)
