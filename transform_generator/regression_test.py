import argparse
import csv
import filecmp
import itertools
import os
import sys

from transform_generator.DdlPathParameters import DdlPathParameters
from transform_generator.generate_data_lineage import generate_data_lineage
from transform_generator.generate_ddl_output import generate_ddl_output
from transform_generator.generate_params_create_dbs import main as generate_params_create_dbs
from transform_generator.generate_sql_output import generate_sql_output
from transform_generator.lib.datafactory.orchestration_pipeline import generate_orchestration_pipeline
from transform_generator.lib.logging import get_logger
from transform_generator.lib.utils import clear_dir
from transform_generator.plugin.loader import get_plugin_loader

import pickle

logger = get_logger(__name__)


def generate_and_compare(project_config_dir: str, actual_output_dir: str, expected_output_dir: str,
                         project_base_dir: str) -> bool:
    """
    This function generates output for the specified TG branch/tag (sql, ddl, data lineage, orchestration)
    and calls the compare function on it and the regression_test_dataset repo
    @param project_config_dir: Path to directory containing project configuration file
    @param actual_output_dir: Path to desired location for storing newly generated output
    @param expected_output_dir: Path to directory containing expected output (regression_test_datasets_repo/output)
    @param project_base_dir: Path to directory containing all project repositories
    @return: Boolean value of True (if the files matched) or False (if the files mismatched)
    """
    if not os.path.exists(actual_output_dir):
        os.makedirs(actual_output_dir)
    clear_dir(actual_output_dir)

    project_config_file_list = os.listdir(project_config_dir)
    if len(project_config_file_list) != 1:
        raise ValueError('The project_config folder should contain only 1 csv file: project config')
    project_config_file_path = os.path.join(project_config_dir, project_config_file_list[0])

    proj_grp_loader = get_plugin_loader().project_group_loader()
    project_group = proj_grp_loader.load_project_group(project_config_file_path, project_base_dir)

 #   with open('project_group.pkl', 'rb') as f:
 #       pickle.dump(project_group, f)
 #       project_group = pickle.load(f)


    databricks_folder_location = '/Shared/cicd'
    databricks_param_prefix = 'parameterization'
    external_module_config_paths = ''

    logger.info('Starting output generation')

    generate_sql_output(project_group,
                        actual_output_dir,
                        actual_output_dir,
                        databricks_folder_location,
                        databricks_param_prefix)

    for project in project_group:
        actual_output_project_dir = os.path.join(actual_output_dir, project.name)
        # ^ path to individual projects' output folders within actual_output_dir

        project_path = os.path.join(project_base_dir, project.name)

        config_path = os.path.join(project_path, 'config')
        schema_path = os.path.join(project_path, 'schema')
        mapping_sheet_path = os.path.join(project_path, 'mapping')

        paths_data_object = DdlPathParameters(config_path, schema_path, mapping_sheet_path, project_config_file_path,
                                              actual_output_project_dir, actual_output_project_dir,
                                              databricks_folder_location, external_module_config_paths)
#        generate_ddl_output(paths_data_object, databricks_param_prefix)

    #generate_orchestration_pipeline(project_group, actual_output_dir, project_config_file_path,
    #                                databricks_param_prefix)
    #generate_params_create_dbs(project_config_file_path, actual_output_dir)

    #generate_data_lineage(project_group, project_config_file_path, actual_output_dir)

    logger.info('Completed output generation')
    outputs_match = compare(expected_output_dir, actual_output_dir)
    logger.info('Completed output comparison')
    return outputs_match


def get_file_set(input_path: str) -> set:
    """
    This function takes a path to an output folder, then uses os.walk() to traverse the folder tree and return file path
    @param input_path: Absolute path to output folder
    @return: A set with all the files and a dictionary with each file mapped to a directory
    """
    logger.info('Started walking all sub-directories and files in ' + input_path)
    file_set = set()

    for path, directory, files in os.walk(input_path):
        for file in files:
            rel_dir = os.path.relpath(path, input_path)
            file_rel_path = os.path.join(rel_dir, file)
            file_set.add(file_rel_path)
    return file_set


def compare_csv(expected_output_file_path, actual_output_file_path):
    with open(expected_output_file_path) as f1, open(actual_output_file_path) as f2:
        expected = csv.reader(f1)
        actual = csv.reader(f2)
        for left, right in itertools.zip_longest(expected, actual):
            if left != right:
                return False
        return True


def compare(expected_output_dir: str, actual_output: str) -> bool:
    """
    This function uses filecmp to compare the files of each output directory
    @param expected_output_dir: Path to regression_test_datasets repo output directory
    @param actual_output: Path to locally generated output directory
    @return: Boolean True or False if output files matched
    """
    logger.info('Started file comparison')
    expected_output_files_set = get_file_set(expected_output_dir)
    actual_files_set = get_file_set(actual_output)
    common_files = expected_output_files_set.intersection(actual_files_set)
    files_content_mismatch = []

    for file in common_files:
        expected_output_file_path = os.path.join(expected_output_dir, file)
        actual_output_file_path = os.path.join(actual_output, file)
        if os.path.splitext(file)[1] == ".csv":
            if not compare_csv(expected_output_file_path, actual_output_file_path):
                files_content_mismatch.append((expected_output_file_path, actual_output_file_path))
        else:
            if not filecmp.cmp(expected_output_file_path, actual_output_file_path, shallow=False):
                files_content_mismatch.append((expected_output_file_path, actual_output_file_path))

    files_not_in_actual_output = expected_output_files_set - actual_files_set
    files_not_in_expected_output = actual_files_set - expected_output_files_set

    if files_content_mismatch or files_not_in_actual_output or files_not_in_expected_output:
        logger.info('Output files in expected output but not in actual output: %s', files_not_in_actual_output)
        logger.info('Num files in expected output but not in actual output: %s', len(files_not_in_actual_output))
        logger.info('Output files in actual output but not in expected output: %s', files_not_in_expected_output)
        logger.info('Num files in actual output but not in expected output: %s', len(files_not_in_expected_output))
        logger.info('Files in both expected and actual output but contents mismatch: %s', files_content_mismatch)
        logger.info('Num files that have mismatching contents: %s', len(files_content_mismatch))
        logger.info('Num files with matching contents: %s', (len(common_files) - len(files_content_mismatch)))
        return False
    else:
        logger.info('All outputs and contents of common_files matched')
        return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create output for regression')
    parser.add_argument('--project_config_dir',
                        help='Path to directory containing project configuration file')
    parser.add_argument('--actual_output_dir', help='Path to desired location for storing newly generated output')
    parser.add_argument('--expected_output_dir', help='Path to directory containing pre-generated output')
    parser.add_argument('--project_base_dir', help='Path to directory containing project repositories')
    args = parser.parse_args()
    reg_test = generate_and_compare(args.project_config_dir, args.actual_output_dir, args.expected_output_dir,
                                    args.project_base_dir)
    if reg_test:
        sys.exit(0)
    else:
        sys.exit(1)
