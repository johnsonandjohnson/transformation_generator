import argparse
from transform_generator.lib.config import get_new_config_structures
from transform_generator.lib.dependency_analysis import get_table_dependency_edges_from_configs, \
    get_field_dependency_edges_from_configs, build_dependency_graph, get_all_upstream_tables, get_all_downstream_tables
from transform_generator.project import Project
from transform_generator.plugin.loader import get_plugin_loader
from transform_generator.writer.dependency_writer import write_dependency
from os import path, makedirs
import csv


def generate_data_lineage(project_group: list[Project], project_config_paths: str, output: str):
    """
    Function to generate the data lineage files
    @param project_group: A list of Project objects
    @param project_config_paths: A semicolon delimited string of paths to the project config files
    @param output: A string path to the base directory where the sql files are saved.
    @param project_config_file_paths: An optional starting list of project config paths
    """
    config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, _, project_config, \
        mappings_by_mapping_filename = get_new_config_structures(project_group, '', project_config_paths, '')

    config_entries = []
    for configs in configs_by_config_filename.values():
        for cfg in configs:
            config_entries.append(cfg)

    table_dependency_edges = get_table_dependency_edges_from_configs(config_entries, mappings_by_mapping_filename,
                                                                     data_lineage=True)
    table_dependency_edges = sorted(table_dependency_edges)

    if not path.exists(output):
        makedirs(output)

    table_output_path = path.join(output, 'table_dependencies.csv')
    write_dependency(table_dependency_edges, table_output_path, config_filename_by_target_table, project_config)

    asts_by_field_dependency_edges = get_field_dependency_edges_from_configs(config_by_mapping_filename.values(),
                                                                             mappings_by_mapping_filename)
    field_dependency_edges = sorted(asts_by_field_dependency_edges.keys())

    field_output_path = path.join(output, 'field_dependencies.csv')
    write_dependency(field_dependency_edges, field_output_path, dependency_type='field')

    dependency_graph = build_dependency_graph(table_dependency_edges)

    write_impacted_tables(dependency_graph, output)


def write_impacted_tables(dependency_graph, output):
    up_downstream_dict = {}
    for table, dependencies in dependency_graph.items():
        upstream = get_all_upstream_tables(table, dependency_graph)
        downstream = get_all_downstream_tables(table, dependency_graph)
        up_downstream_dict[table] = (sorted(upstream), sorted(downstream))
    up_downstream_output_path = path.join(output, 'impacted_tables.csv')
    with open(up_downstream_output_path, 'w', newline='') as csvfile:
        field_names = ['table_name', 'upstream_tables', 'downstream_tables']
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        for table, dependencies in up_downstream_dict.items():
            upstream_deps, downstream_deps = dependencies
            writer.writerow({field_names[0]: table,
                             field_names[1]: (', '.join(upstream_deps) if upstream_deps else None),
                             field_names[2]: (', '.join(downstream_deps) if downstream_deps else None)})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process data mapping files.')
    parser.add_argument('--project_paths', help='Semicolon (;) delimited string of paths to project directories containing config and mapping directories')
    parser.add_argument('--project_base_dir', help='Path to directory containing project repositories')
    parser.add_argument('--output', help='Base output directory for SQL files and Bash scripts')
    args = parser.parse_args()

    project_loader = get_plugin_loader().project_group_loader()
    project_group = project_loader.load_project_group(args.project_paths, args.project_base_dir)

    generate_data_lineage(project_group, args.project_paths, args.output)
