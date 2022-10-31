import collections
import json
import shutil
from os import path, makedirs, remove
from typing import Set, List

from transform_generator.lib.logging import get_logger
from transform_generator.lib.config import get_new_config_structures, get_configs_mappings_from_data_mappings
from transform_generator.lib.dependency_analysis import get_dependency_graph_for_configs, \
    get_dependency_graph_for_modules
from transform_generator.lib.datafactory.pipeline import generate_main_adf_pipeline
from transform_generator.reader.project_config_reader import read_project_config_csvs

from transform_generator.project import Project

logger = get_logger(__name__)


def generate_pipeline(project_group: list[Project], output_dir: str, project_config_path: str,
                      database_param_prefix: str, external_module_config_paths: str, modules: Set[str]):
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_new_config_structures(project_group, database_param_prefix,
                                                                                 project_config_path,
                                                                                 external_module_config_paths)

    config_dependency_graph = get_dependency_graph_for_configs(configs_by_config_filename, mappings_by_mapping_filename,
                                                               config_filename_by_target_table, project_config,
                                                               modules)

    module_dependency_graph = get_dependency_graph_for_modules(config_dependency_graph, project_config,
                                                               database_param_prefix, database_variables)

    main_pipeline_name = 'core_semantic_transformations'
    main_pipeline_folder = 'ConsumerMain'

    if not path.exists(path.join(output_dir, "ConsumerMain")):
        makedirs(path.join(output_dir, "ConsumerMain"))

    if database_variables:
        if database_param_prefix != "":
            if path.exists(path.join(output_dir, "ConsumerMain", main_pipeline_name + ".json")):
                remove(path.join(output_dir, "ConsumerMain", main_pipeline_name + ".json"))
            if path.exists(path.join(output_dir, "ConsumerMain", "Main_Data_Pipeline.json")):
                remove(path.join(output_dir, "ConsumerMain", "Main_Data_Pipeline.json"))
        main_pipeline_name = database_param_prefix + '_' + main_pipeline_name
        main_pipeline_folder = database_param_prefix
    main_pipeline = generate_main_adf_pipeline(main_pipeline_name, main_pipeline_folder, module_dependency_graph,
                                               project_config)

    with open(path.join(output_dir, "ConsumerMain", main_pipeline_name + '.json'), 'w') as outfile:
        json.dump(main_pipeline, outfile, indent=4)


def generate_ddl_orchestration_pipeline(project_group: list[Project], output_dir: str, database_param_prefix: str):
    """
    Function to independently create the DDL Orchestration ADF Pipeline
    @param project_group: list of Project objects
    @param output_dir: string path to the directory to save the generated pipeline
    @param project_config_paths: string path to the generator/project level config file
    @param database_param_prefix: A string containing the prefix for the ADF Pipelines if any database parameterization
        flags are Y
    """
    _, configs_by_config_filename, project_config, _ = get_configs_mappings_from_data_mappings(project_group)

    database_variables = False
    for project_config_entry in project_config.values():
        if project_config_entry.parallel_db_name != '':
            database_variables = True

    configs_by_config_filename = collections.OrderedDict(sorted(configs_by_config_filename.items()))

    modules = {}
    for config_file_name, configs in configs_by_config_filename.items():
        module_name = project_config[config_file_name].group_name + '_ddl'
        if database_variables:
            module_name = database_param_prefix + '_' + module_name
        modules[module_name] = set()

    ddl_pipeline_folder = 'DDL'
    ddl_pipeline_name = 'ddl'

    ddl_output_file_path = path.join(output_dir, ddl_pipeline_folder, ddl_pipeline_name + '.json')
    if database_variables:
        if database_param_prefix != "" and path.exists(ddl_output_file_path):
            remove(ddl_output_file_path)
        ddl_pipeline_name = database_param_prefix + '_' + ddl_pipeline_name
        ddl_pipeline_folder = database_param_prefix
        ddl_output_file_path = path.join(output_dir, ddl_pipeline_folder, ddl_pipeline_name + '.json')

    ddl_pipeline = generate_main_adf_pipeline(ddl_pipeline_name, ddl_pipeline_folder, modules, project_config,
                                              processing_date=False)

    if not path.exists(path.join(output_dir, ddl_pipeline_folder)):
        makedirs(path.join(output_dir, ddl_pipeline_folder))
    with open(ddl_output_file_path, 'w') as outfile:
        json.dump(ddl_pipeline, outfile, indent=4)


def generate_orchestration_pipeline(project_group: list[Project], orchestration_output_dir: str,
                                    project_config_path: str, database_param_prefix: str = '',
                                    external_module_config_paths: str = ''):
    output_dir = path.join(orchestration_output_dir, "datafactory", "pipelines", "generated")
    if not path.exists(output_dir):
        makedirs(output_dir)

    modules = {project.name for project in project_group}

    if project_group:
        generate_pipeline(project_group, output_dir, project_config_path, database_param_prefix,
                          external_module_config_paths, modules)
        generate_ddl_orchestration_pipeline(project_group, output_dir, database_param_prefix)
    else:
        logger.warn("Orchestration pipeline NOT generated. No configurations defined for this orchestration repository")
        logger.warn("Orchestration DDL pipeline NOT generated. "
                    "No configurations defined for this orchestration repository or the dependent repositories")
