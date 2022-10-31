from os.path import join
from typing import List

from transform_generator.project import Project, load_project
from transform_generator.reader.project_config_reader import read_project_config_csvs


def load_project_group(project_config_path: str, project_base_path: str) -> List[Project]:
    """
    Loads a project configuration file and loads each project specified from the project base path.
    Each projects path is the project base path joined with the project name as defined in the config file. Each
    project is loaded into a Project object, and used to construct a ProjectGroup object
    .
    @param project_config_path: The full path to the project configuration file.
    @param project_base_path: The path to the project base directory, which contains all project directories.
    @return: A Project Group containing all of the configured projects.
    """
    cfg_entries = read_project_config_csvs(project_config_path.split(';'))

    entries_by_group_name = {}

    for entry in cfg_entries.values():
        entries = entries_by_group_name.get(entry.group_name, [])
        entries.append(entry)
        entries_by_group_name[entry.group_name] = entries

    entries_by_group_name.pop('ciw', None)  # temp; no config dir in ciw repo

    return [load_project(join(project_base_path, entries[0].group_name), entries[0].group_name, entries)
            for entries in entries_by_group_name.values()]
