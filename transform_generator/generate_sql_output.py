import argparse
import json
from os import path, makedirs
from typing import Dict, List, Tuple

from transform_generator import executor
from transform_generator.lib.config import get_config_structures
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.datafactory.pipeline import generate_datafactory_pipeline, \
    get_notebook_paths_by_target_table, \
    rename_adf_activities, generate_module_adf_pipeline
from transform_generator.lib.dependency_analysis import get_table_dependency_edges_from_configs, build_dependency_graph
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.logging import get_logger
from transform_generator.lib.sql_scripts import generate_sql_scripts, get_db_table_name
from transform_generator.plugin import loader
from transform_generator.plugin.dc_databricks_notebooks import GenerateDcTransformationNotebooks, \
    GenerateDcTableViewNotebooks
from transform_generator.plugin.generate_action import GenerateFilesAction
from transform_generator.plugin.generate_databricks_notebooks import GenerateTransformationNotebooks, \
    GenerateTableViewCreateNotebooks
from transform_generator.project import Project
from transform_generator.reader.table_definition_reader import get_table_definition

logger = get_logger(__name__)

def generate_sql_output(project_group: list[Project], output_databricks: str,
                        output_datafactory: str, folder_location: str,
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
    dml_stage = GenerateDcTransformationNotebooks("dml")
    ddl_stage = GenerateDcTableViewNotebooks("ddl")
    executor.execute_stages(project_group,
                            [dml_stage, ddl_stage],
                            target_output_dir=output_databricks,
                            dc_super_argument='blah')

    # config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
    #     project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_sheet_path,
    #                                                                          database_param_prefix,
    #                                                                          project_config_paths,
    #                                                                          external_module_config_paths)
    #
    # modules = set()
    # for config_file_name, configs in configs_by_config_filename.items():
    #     group_name = project_config[config_file_name].group_name
    #     modules.add(group_name)
    #
    #     folder_path = ''
    #     if group_name != '':
    #         folder_path += '/' + group_name
    #
    #     folder_path += '/databricks/generated/dml'
    #
    #     notebook_paths_by_target_table = get_notebook_paths_by_target_table(
    #         configs_by_config_filename[config_file_name], filenames_by_target_table,
    #         folder_location, folder_path, database_variables)
    #
    #     sequencing = {config_entry.target_table: config_entry.sequence for config_entry in configs if
    #                   config_entry.sequence}
    #
    #     # Compute dependency graph just for this config file (workstream)
    #     dependency_edges = get_table_dependency_edges_from_configs(configs, mappings_by_mapping_filename)
    #     dependency_graph = build_dependency_graph(dependency_edges, sequencing)
    #
    #     cluster_name = project_config[config_file_name].cluster_name
    #     if not cluster_name:
    #         cluster_name = 'HighConCluster'
    #
    #     if dependency_graph:
    #         if database_variables:
    #             pipeline = generate_datafactory_pipeline(dependency_graph, notebook_paths_by_target_table,
    #                                                      config_file_name, project_config, cluster_name,
    #                                                      database_param_prefix)
    #         else:
    #             pipeline = generate_datafactory_pipeline(dependency_graph, notebook_paths_by_target_table,
    #                                                      config_file_name, project_config, cluster_name)
    #
    #         if pipeline:
    #             output_adf_dir = path.join(output_datafactory, 'datafactory', 'pipelines', 'generated')
    #             adf_file_name = rename_adf_activities(config_file_name).replace('config_', '').replace('-csv', '.json')
    #             if "core" in adf_file_name:
    #                 output_adf_dir = path.join(output_adf_dir, 'CorePipelines')
    #             else:
    #                 output_adf_dir = path.join(output_adf_dir, 'SemanticPipelines')
    #             output_adf_file_path = path.join(output_adf_dir, adf_file_name)
    #
    #             if not path.exists(output_adf_dir):
    #                 makedirs(output_adf_dir)
    #
    #             # Save pipeline json
    #             with open(output_adf_file_path, 'w') as outfile:
    #                 json.dump(pipeline, outfile, indent=4)
    #
    # if len(modules) == 1:
    #     # Create module-level ADF Pipeline
    #     main_pipeline, module_pipeline_path = generate_module_adf_pipeline(config_filename_by_target_table,
    #                                                                        configs_by_config_filename,
    #                                                                        database_param_prefix, database_variables,
    #                                                                        project_config, group_name,
    #                                                                        mappings_by_mapping_filename,
    #                                                                        output_datafactory, modules)
    #     with open(module_pipeline_path, 'w') as outfile:
    #         json.dump(main_pipeline, outfile, indent=4)
    #     logger.info('\tPipeline output file %s', module_pipeline_path)
    # else:
    #     logger.info('\tPipeline not created, %s contains config files with more than one module.', config_path)


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
