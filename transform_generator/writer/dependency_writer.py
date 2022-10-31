import csv
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from typing import Dict


def write_dependency(dependency_edges, output_file_path, config_filename_by_target_table: Dict[str, str] = {},
                     project_config: Dict[str, ProjectConfigEntry] = {},
                     dependency_type='table'):
    """
                Write the dependency information onto a .CSV file.
                Add two extra columns: 'source_project' and target_project' when dependency_type='table'

                :param dependency_edges: - A simple list of dependency tuple. The tuple list contains combination of
                                              source and target tables.
                :param output_file_path: - A path to the output file.
                :param config_filename_by_target_table: - Used for compatibility to get source and target projects.
                :param project_config: -  Dictionary of ProjectConfigEntry to find project for source & target tables.
                :param dependency_type: - String to identify .CSV file is for field dependency or table dependency.


        """

    with open(output_file_path, 'w', newline='') as csvfile:
        field_names = ["source", "target"]
        if dependency_type == 'table':
            field_names += ["source_project", "target_project"]
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        for source_table, target_table in dependency_edges:
            row = {field_names[0]: source_table, field_names[1]: target_table}
            if dependency_type == 'table':
                row[field_names[2]] = source_table in config_filename_by_target_table and \
                              project_config[config_filename_by_target_table[source_table]].group_name or ""
                row[field_names[3]] = project_config[config_filename_by_target_table[target_table]].group_name
            writer.writerow(row)
