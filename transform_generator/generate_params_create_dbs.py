from os import path, makedirs
import argparse
from typing import Dict, List, Set

from transform_generator.lib.databricks import create_sql_widget
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.reader.project_config_reader import read_project_config_csvs


def get_unique_databases(project_config: Dict[str, ProjectConfigEntry]) -> Set[str]:
    """
    This function creates of set of variable databases from the generator config file
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @return: A set of the variable databases
    """
    databases = set()
    for project_config_entry in project_config.values():
        parallel_db_name = project_config_entry.parallel_db_name
        if project_config_entry.parallel_db_name != '':
            databases.add('${' + parallel_db_name + '_db}')
    return databases


def get_create_database(databases: Set[str]) -> List[str]:
    """
    This function creates a list of create database statements for all the unique variable databases
    @param databases: A set of strings of the unique variable databases
    @return: A list of create database sql statements
    """
    create_database = 'CREATE DATABASE IF NOT EXISTS '
    create_databases_sql = []
    for database in databases:
        create_databases_sql.append(create_database + database)
    return create_databases_sql


def create_sql_notebook(widget_text: str, create_databases: List[str]) -> str:
    """
    This function combines the necessary text for the create databases sql file
    @param widget_text: The text with the sql widgets
    @param create_databases: A list of the create databases statements
    @return: A string containing all the sql code for the create databases sql file
    """
    sql_text = '\n\n-- COMMAND --\n\n'.join(create_databases)
    sql_text = '\n\n-- COMMAND --\n\n'.join([widget_text, sql_text])
    return sql_text


def main(project_config_paths: str, output: str):
    """
    This function takes in two paths and creates the notebook containing all the necessary create database statements
    for the variable databases
    @param project_config_paths: A semicolon delimited string of paths to the project config files
    @param output: A string path to the base directory where the sql files are saved.
    """
    project_config_paths = project_config_paths.split(';')
    project_config = read_project_config_csvs(project_config_paths)

    databases = get_unique_databases(project_config)
    create_database_sql = get_create_database(databases)

    widget_text = create_sql_widget(project_config)

    sql_notebook = create_sql_notebook(widget_text, create_database_sql)

    output_file_dir = path.join(output, 'databricks', 'create_databases')
    if not path.exists(output_file_dir):
        makedirs(output_file_dir)
    with open(output_file_dir + '/create_databases.sql', 'w') as f:
        f.write(sql_notebook)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process data mapping files.')
    parser.add_argument('--project_config_paths',
                        help='Semicolon (;) delimited string of paths to project config files')
    parser.add_argument('--output', help='Base output directory for SQL files')
    args = parser.parse_args()

    main(args.project_config_paths, args.output)
