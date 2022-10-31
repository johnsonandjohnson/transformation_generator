import collections

from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.config import get_target_table_partition_key
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.parser.transform_exp import get_field_dependencies_for_ast
from typing import Dict, List, Set, Tuple
from transform_generator.lib.logging import get_logger

logger = get_logger(__name__)


def build_dependency_graph(dependency_edges, sequencing=dict()):
    """
    Build the dependency graph based on the dependency tuple
    @param dependency_edges: a list of dependency tuples. The tuples list contains combination of source and target
        tables.
    @param sequencing: a dictionary of a sequencing number to each target_table, optional, the higher the number, the
        higher the priority (i.e. 2 > 1 so the one with 2 comes first)
    @return: dependencies: A dictionary containing a tuple that contains the source tables and target tables for each
        table (both src and tgt), which is the dependency information.
    """
    dependencies = {}

    for src, tgt in dependency_edges:
        dependencies[src] = dependencies.get(src, set())

        tgt_depend = dependencies.get(tgt, set())
        if tgt not in sequencing or src not in sequencing or sequencing[src] < sequencing[tgt]:
            tgt_depend.add(src)

        dependencies[tgt] = tgt_depend

    return collections.OrderedDict(sorted(dependencies.items()))


def generate_graphviz(dependency_map, output_path):
    from graphviz import Digraph

    dot = Digraph(comment="Table dependency graph")
    for label, dependencies in dependency_map.items():
        dot.node(label)
        for dependency in dependencies:
            dot.edge(dependency, label)

    print(dot.source)
    dot.render(output_path, view=True)


def get_tables_in_dependency_order(dependency_map):
    """
    Arrange tables in the order of their dependencies and returns a list of sets of tables showing the table dependency
        orders.
    @param dependency_map: A dictionary with a key of table name or config name, and a value of a set of its dependents.
    @return: result: A list containing sets of tables arranged in their dependency order.
    """
    result = []
    visited_nodes = set()
    while len(visited_nodes) < len(dependency_map):
        dependency_free_set = set()
        for key, item in dependency_map.items():
            if key not in visited_nodes and item - visited_nodes == set():
                dependency_free_set.add(key)
        visited_nodes = visited_nodes | dependency_free_set
        result.append(dependency_free_set)

    return result[1:]


def get_table_dependency_edges_from_config_mapping(
        config: ConfigEntry, target_table_region_key: str,
        mappings_by_mapping_filename: Dict[str, DataMapping]) -> Set[Tuple[str, str]]:
    single_config_dependency_edges = set({})

    for mapping_filename in config.input_files:
        for table_dependency in mappings_by_mapping_filename[mapping_filename].table_dependencies:
            # We must use the get_target_table_partition_key function to
            # account for target tables which have multiple partitions.
            # This applies only to target tables.
            if table_dependency != target_table_region_key:
                single_config_dependency_edges.add((table_dependency, target_table_region_key))

    return single_config_dependency_edges


def get_table_dependency_edges_from_config_dependencies(config: ConfigEntry,
                                                        target_table_region_key: str) -> Set[Tuple[str, str]]:
    single_config_dependency_edges = set({})
    for dependency in set(config.dependencies.split(',')):
        if dependency and dependency != target_table_region_key:
            single_config_dependency_edges.add((dependency, target_table_region_key))

    return single_config_dependency_edges


def get_table_dependency_edges_from_configs(configs: List[ConfigEntry], mappings_by_mapping_filename: Dict[
        str, DataMapping], data_lineage: bool = False) -> Set[Tuple[str, str]]:
    dependency_edges = set()
    for config in configs:
        target_table_region_key = get_target_table_partition_key(config)
        if config.target_type == 'program' or config.dependencies:
            config_dependency_edges = get_table_dependency_edges_from_config_dependencies(config,
                                                                                          target_table_region_key)
            dependency_edges = config_dependency_edges.union(dependency_edges)

        if config.target_type not in {'program', 'lineage'} or (data_lineage and config.target_type == 'lineage'):
            mapping_edges = get_table_dependency_edges_from_config_mapping(config, target_table_region_key,
                                                                           mappings_by_mapping_filename)
            dependency_edges = mapping_edges.union(dependency_edges)

    return dependency_edges


def rename_adf_config_activities(activity_str: str) -> str:
    """
    This function takes in a string file or activity name and replaces characters to make
    it compliant with Azure Data Factory naming conventions, and it removes config_ and .csv
    :param activity_str: String of activity name to edit
    :return: String of processed activity name
    """
    return activity_str.replace('config_', '').replace('.csv', '').replace('.', '-')


def get_single_config_table_dependencies(
        config: ConfigEntry,
        config_filename: str,
        config_filename_by_target_table: Dict[str, str],
        project_config: Dict[str, ProjectConfigEntry],
        modules: Set[str] = {}) -> Set[Tuple[str, str]]:
    dependency_edges = set({})
    for table_dependency in config.dependencies.upper().split(','):
        if table_dependency in config_filename_by_target_table:
            src_config_filename = config_filename_by_target_table[table_dependency]
            if modules and (project_config[src_config_filename].group_name not in modules or project_config[
                    config_filename].group_name not in modules):
                break
            src_config_filename = rename_adf_config_activities(src_config_filename)
            tgt_config_filename = rename_adf_config_activities(config_filename)
            if tgt_config_filename != src_config_filename:
                dependency_edges.add((src_config_filename, tgt_config_filename))

    return dependency_edges


def get_single_config_mapping_file_dependencies(
        mapping: DataMapping,
        config_filename: str,
        config_filename_by_target_table: Dict[str, str],
        project_config: Dict[str, ProjectConfigEntry],
        modules: Set[str] = {}) -> Set[Tuple[str, str]]:
    dependency_edges = set({})
    for table_dependency in mapping.table_dependencies:
        if table_dependency in config_filename_by_target_table:
            src_config_filename = config_filename_by_target_table[table_dependency]
            if modules and project_config[src_config_filename].group_name in modules and project_config[
                    config_filename].group_name in modules:
                src_config_filename = rename_adf_config_activities(src_config_filename)
                tgt_config_filename = rename_adf_config_activities(config_filename)
                if tgt_config_filename != src_config_filename:
                    dependency_edges.add((src_config_filename, tgt_config_filename))

    return dependency_edges


def get_config_dependency_edges_from_single_config(
        config: ConfigEntry,
        config_filename: str,
        mappings_by_mapping_filename: Dict[str, List[DataMapping]],
        config_filename_by_target_table: Dict[str, str],
        project_config: Dict[str, ProjectConfigEntry],
        modules: Set[str] = {}) -> Set[Tuple[str, str]]:
    dependency_edges = set({})
    target_type = config.target_type.lower() if config.target_type else ''
    if target_type in {'program', 'lineage'} or config.dependencies:
        config_dependency_edges = get_single_config_table_dependencies(config, config_filename,
                                                                       config_filename_by_target_table,
                                                                       project_config, modules)
        dependency_edges = config_dependency_edges.union(dependency_edges)

    if target_type not in {'view', 'program', 'lineage'}:
        for mapping_filename in config.input_files:
            mapping = mappings_by_mapping_filename[mapping_filename]
            mapping_dependency_edges = get_single_config_mapping_file_dependencies(mapping, config_filename,
                                                                                   config_filename_by_target_table,
                                                                                   project_config, modules)
            dependency_edges = mapping_dependency_edges.union(dependency_edges)

    return dependency_edges


def get_config_dependency_edges_from_configs(
        configs_by_config_filename: Dict[str, List[ConfigEntry]],
        mappings_by_mapping_filename: Dict[str, List[DataMapping]],
        config_filename_by_target_table: Dict[str, str],
        project_config: Dict[str, ProjectConfigEntry],
        modules: Set[str] = {}) -> Set[Tuple[str, str]]:
    dependency_edges = set({})
    for config_filename, configs in configs_by_config_filename.items():
        for config in configs:
            config_dependencies = get_config_dependency_edges_from_single_config(config, config_filename,
                                                                                 mappings_by_mapping_filename,
                                                                                 config_filename_by_target_table,
                                                                                 project_config, modules)
            dependency_edges = config_dependencies.union(dependency_edges)

    return dependency_edges


def get_field_dependency_edges_from_configs(configs: List[ConfigEntry],
                                            mappings_by_mapping_filename: Dict[str, DataMapping]) -> Dict[
                                                Tuple[str, str], str]:
    """
    For all data mappings in a list of configuration files, return the field dependency edges and corresponding rules.
    @param configs: A list of all configuration files to produce dependency information for.
    @param mappings_by_mapping_filename: A dictionary with the data mapping filename as the key and the DataMapping as
        the value.
    @return: A dictionary with (source field name, target field name) as a key and a string representing the
        TransformExp for the target column.
    """
    dependency_edges = {}
    for config in configs:
        for mapping_filename in config.input_files:
            mapping = mappings_by_mapping_filename[mapping_filename]
            for target_col in mapping.target_column_names:
                ast = mapping.ast(target_col)

                try:
                    field_dependencies = get_field_dependencies_for_ast(ast)
                except Exception as err:
                    logger.error(mapping_filename + ": " + err.args[0])
                    continue

                for qualified_source_col in field_dependencies:
                    qualified_target_col = mapping.database_name + "." + mapping.table_name + "." + target_col
                    dependency_edges[(qualified_source_col, qualified_target_col)] = str(ast)

    return dependency_edges


def get_dependency_graph_for_configs(configs_by_config_filename: Dict[str, List[ConfigEntry]],
                                     mappings_by_mapping_filename: Dict[str, List[DataMapping]],
                                     config_filename_by_target_table: Dict[str, str],
                                     project_config: Dict[str, ProjectConfigEntry],
                                     modules: Set[str] = {}) -> Dict[str, Set[str]]:
    """
    This function creates the dependency graph at the config file level.
    @param configs_by_config_filename: A dictionary where the key is a config filename string and the values are lists
        of config entries.
    @param mappings_by_mapping_filename: A dictionary where the key is a mapping filename string and the values are
        lists of data mappings.
    @param config_filename_by_target_table: A dictionary where the key is a target table string and the value is a
        config filename string.
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param modules: Optional, a set of strings containing the module that is currently being run
    @return: A dictionary where the key is a config file string and the value is a set of other config file strings that
        the key depends on.
    """
    config_dependency_edges = get_config_dependency_edges_from_configs(configs_by_config_filename,
                                                                       mappings_by_mapping_filename,
                                                                       config_filename_by_target_table,
                                                                       project_config, modules)

    sequencing = {rename_adf_config_activities(name): entry.sequence for name, entry in
                  project_config.items() if entry.sequence}

    config_dependency_graph = build_dependency_graph(config_dependency_edges, sequencing)

    for config_filename, configs in configs_by_config_filename.items():
        filename = config_filename.replace('.csv', '').replace('config_', '').replace('.', '-')
        if filename not in config_dependency_graph:
            target_types = set()
            for config in configs:
                target_types.add(config.target_type)
            if target_types not in [{'view'}, {'lineage'}, {'view', 'lineage'}]:
                config_dependency_graph[filename] = set()

    return collections.OrderedDict(sorted(config_dependency_graph.items()))


def get_all_upstream_tables(table, dependency_graph):
    """
    This function computes all the upstream dependencies of the specified table
    :param table: a table in the dependency graph in string format
    :param dependency_graph: a dictionary where the key is a table and the value is a list of tables directly upstream
        of the key table
    :return: visited: A set containing all the tables that are upstream of table
    """
    queue = list(dependency_graph[table])
    visited = set()
    while queue:
        direct_upstream = dependency_graph[queue[0]]
        for tbl in direct_upstream:
            if tbl not in queue and tbl not in visited:
                queue.append(tbl)
        visited.add(queue[0])
        del queue[0]
    return visited


def get_all_downstream_tables(table, dependency_graph):
    """
    This function inverts the given dependency graph and calls function get_all_upstream_tables to compute a set
    containing all downstream dependencies of the specified table
    :param table: a table in the dependency graph in string format
    :param dependency_graph: a dictionary where the key is a table and the value is a list of tables directly upstream
        of the key table
    :return: a set containing all the tables that are downstream of table
    """
    # invert the dependency graph such that key = table, value = list of direct downstream tables
    inverted_dependency_graph = {key: set() for key in dependency_graph.keys()}
    # initializing graph for all keys; otherwise, tables without any downstream tables will not be accounted for
    for key, value in dependency_graph.items():
        for tbl in value:
            inverted_dependency_graph[tbl].add(key)
    return get_all_upstream_tables(table, inverted_dependency_graph)


def identify_cyclic_dependencies(dependency_graph: Dict):
    """
    This function identifies cyclic dependencies.
    @param dependency_graph: A dictionary with the key being every table and the values being a tuple containing a set
        of the source tables and a set of the target tables that make up the table dependencies"""
    cyclic_dependencies = set()
    for table in dependency_graph.keys():
        if table in get_all_upstream_tables(table, dependency_graph) \
                or table in get_all_downstream_tables(table, dependency_graph):
            cyclic_dependencies.add(table)
    return cyclic_dependencies


def get_new_dependency_from_dependency(dependency: str, module_name: str,
                                       project_config: Dict[str, ProjectConfigEntry], database_param_prefix: str,
                                       database_variables: bool = False) -> str:
    """
    Determine if the provided dependency has a dependency of its own, and return a string representation of it
    @param dependency: a dependency name that will be checked for an additional dependency
    @param module_name: the name of the module that the dependency came from
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param database_param_prefix: string of the database parameterization prefix
    @param database_variables: A boolean indicating if database variables is active, defaults to False
    @return: string representation of dependency, or None
    """
    new_dependency = None
    if database_variables:
        filename = (dependency.replace(database_param_prefix,
                                       database_param_prefix + '_config') + '.csv').replace('-', '.')
    else:
        filename = ('config_' + dependency + '.csv').replace('-', '.')
    dependency_module_name = project_config[filename].group_name + '_transformations'
    if dependency_module_name != module_name:
        if database_variables:
            new_dependency = database_param_prefix + '_' + dependency_module_name
        else:
            new_dependency = dependency_module_name

    return new_dependency


def get_dependency_graph_for_modules(config_dependency_graph: Dict[str, Set[str]],
                                     project_config: Dict[str, ProjectConfigEntry], database_param_prefix: str,
                                     database_variables: bool = False) -> Dict[str, Set[str]]:
    """
    Creates module level dependency graph from config_dependency_graph
    @param config_dependency_graph: dependency graph at the configuration file level
    @param project_config: A dictionary with the config filename as the key and generator config entries as the values
    @param database_param_prefix: string of the database parameterization prefix
    @param database_variables: A boolean indicating if database variables is active, defaults to False
    @return: dependency graph at the module level
    """
    module_dependency_graph = {}

    for config_filename, dependencies in config_dependency_graph.items():
        module_dependencies = set()
        if database_variables:
            filename = (config_filename.replace(database_param_prefix,
                                                database_param_prefix + '_config') + '.csv').replace('-', '.')
        else:
            filename = ('config_' + config_filename + '.csv').replace('-', '.')

        module_name = project_config[filename].group_name + '_transformations'
        for dependency in dependencies:
            dependency_module_name = get_new_dependency_from_dependency(dependency, module_name, project_config,
                                                                        database_param_prefix, database_variables)
            if dependency_module_name:
                module_dependencies.add(dependency_module_name)

        if database_variables:
            module_dependency_graph[database_param_prefix + '_' + module_name] = module_dependencies
        else:
            module_dependency_graph[module_name] = module_dependencies
    return module_dependency_graph
