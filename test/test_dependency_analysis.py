import os

import unittest
from transform_generator.lib.config import get_config_structures
from transform_generator.lib.dependency_analysis import build_dependency_graph, get_tables_in_dependency_order, \
    get_all_upstream_tables, get_all_downstream_tables, get_table_dependency_edges_from_configs, \
    get_dependency_graph_for_configs, rename_adf_config_activities


class TestDependencyAnalysis(unittest.TestCase):

    def test_dependency_graph_set1(self):
        dependency_tuples = [('A', 'C'),
                             ('B', 'C'),
                             ('C', 'E'),
                             ('C', 'F'),
                             ('D', 'E')]
        expected_dependency_map = {'A': set(),
                                   'B': set(),
                                   'C': {'A', 'B'},
                                   'D': set(),
                                   'E': {'C', 'D'},
                                   'F': {'C'}}

        dependencies = build_dependency_graph(dependency_tuples)
        self.assertEqual(dependencies, expected_dependency_map)

    def test_dependency_graph_set2(self):
        dependency_tuples = [('B', 'E'),
                             ('D', 'E'),
                             ('C', 'A'),
                             ('A', 'B'),
                             ('C', 'B')]
        expected_dependency_map = {'A': {'C'},
                                   'B': {'A', 'C'},
                                   'C': set(),
                                   'D': set(),
                                   'E': {'B', 'D'}}
        dependencies = build_dependency_graph(dependency_tuples)
        self.assertEqual(dependencies, expected_dependency_map)

    def test_dependency_graph_set3(self):
        dependency_tuples = [('A', 'D'),
                             ('B', 'D'),
                             ('C', 'D'),
                             ('C', 'E'),
                             ('D', 'E')]
        expected_dependency_map = {'A': set(),
                                   'B': set(),
                                   'C': set(),
                                   'D': {'A', 'B', 'C'},
                                   'E': {'C', 'D'}}
        dependencies = build_dependency_graph(dependency_tuples)
        self.assertEqual(dependencies, expected_dependency_map)

    def test_dependency_order_set1(self):
        dependency_graph = {'A': set(),
                            'B': set(),
                            'C': {'A', 'B'},
                            'D': set(),
                            'E': {'C', 'D'},
                            'F': {'C'}}
        table_dependency_order = get_tables_in_dependency_order(
            dependency_graph)
        self.assertNotIn('A', table_dependency_order)
        self.assertNotIn('B', table_dependency_order)
        self.assertNotIn('D', table_dependency_order)
        expected_table_dependency_1_node_set = {'C'}
        self.assertEqual(table_dependency_order[0],
                         expected_table_dependency_1_node_set)
        expected_table_dependency_2_node_set = {'E', 'F'}
        self.assertEqual(table_dependency_order[1],
                         expected_table_dependency_2_node_set)

    def test_dependency_order_set2(self):
        dependency_graph = {'A': {'C'},
                            'B': {'A', 'C'},
                            'C': set(),
                            'D': set(),
                            'E': {'B', 'D'}}
        table_dependency_order = get_tables_in_dependency_order(
            dependency_graph)
        self.assertNotIn('C', table_dependency_order)
        self.assertNotIn('D', table_dependency_order)
        expected_table_dependency_1_node_set = {'A'}
        self.assertEqual(table_dependency_order[0],
                         expected_table_dependency_1_node_set)
        expected_table_dependency_2_node_set = {'B'}
        self.assertEqual(table_dependency_order[1],
                         expected_table_dependency_2_node_set)
        expected_table_dependency_3_node_set = {'E'}
        self.assertEqual(table_dependency_order[2],
                         expected_table_dependency_3_node_set)

    def test_dependency_order_set3(self):
        dependency_graph = {'A': set(),
                            'B': set(),
                            'C': set(),
                            'D': {'A', 'B', 'C'},
                            'E': {'C', 'D'}}
        table_dependency_order = get_tables_in_dependency_order(
            dependency_graph)
        self.assertNotIn('A', table_dependency_order)
        self.assertNotIn('B', table_dependency_order)
        self.assertNotIn('C', table_dependency_order)
        expected_table_dependency_1_node_set = {'D'}
        self.assertEqual(table_dependency_order[0],
                         expected_table_dependency_1_node_set)
        expected_table_dependency_2_node_set = {'E'}
        self.assertEqual(table_dependency_order[1],
                         expected_table_dependency_2_node_set)

    def test_upstream_dependencies(self):
        dependency_graph = {'A': set(),
                            'B': set(),
                            'C': set(),
                            'D': {'A', 'B', 'C'},
                            'E': {'C', 'D'}}
        upstream_dependencies = get_all_upstream_tables('E', dependency_graph)
        expected_upstream_dependencies = {'C', 'D', 'A', 'B'}
        self.assertEqual(upstream_dependencies, expected_upstream_dependencies)

    def test_downstream_dependencies(self):
        dependency_graph = {'A': set(),
                            'B': set(),
                            'C': {'A', 'F'},
                            'D': {'A', 'B', 'C'},
                            'E': {'C'},
                            'F': {'B', 'D'}}
        downstream_dependencies = get_all_downstream_tables('C', dependency_graph)
        expected_downstream_dependencies = {'D', 'E', 'F', 'C'}
        self.assertEqual(downstream_dependencies, expected_downstream_dependencies)

    def test_table_level_cyclic_dependencies(self):
        resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'positive_cases')
        config_path = os.path.join(resources_folder, 'config', )
        mapping_path = os.path.join(resources_folder, 'mapping')
        project_config_path = os.path.join(resources_folder, 'project_config', 'project_config_test.csv')

        config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
        project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_path, '',
                                                                             project_config_path)

        config_filename = 'config_test_cyclic_dependencies.csv'
        configs = configs_by_config_filename[config_filename]
        sequencing = {config_entry.target_table: config_entry.sequence for config_entry in configs if
                      config_entry.sequence}

        dependency_edges = get_table_dependency_edges_from_configs(configs, mappings_by_mapping_filename)
        dependency_graph = build_dependency_graph(dependency_edges, sequencing)

        for table, sequence in sequencing.items():
            for tbl, seq in sequencing.items():
                if sequence >= seq:
                    self.assertNotIn(table, dependency_graph[tbl])

        self.assertIn('TEST.CYCLIC_DEPENDENCY_DIRECT_SRC', dependency_graph['TEST.CYCLIC_DEPENDENCY_DIRECT_TGT'])

        self.assertIn('TEST.CYCLIC_DEPENDENCY_INDIRECT_1', dependency_graph['TEST.CYCLIC_DEPENDENCY_INDIRECT_2'])
        self.assertIn('TEST.CYCLIC_DEPENDENCY_INDIRECT_2', dependency_graph['TEST.CYCLIC_DEPENDENCY_INDIRECT_3'])

    def test_config_level_cyclic_dependencies(self):
        resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources', 'positive_cases')
        config_path = os.path.join(resources_folder, 'config', )
        mapping_path = os.path.join(resources_folder, 'mapping')
        project_config_path = os.path.join(resources_folder, 'project_config', 'project_config_test.csv')

        config_by_mapping_filename, config_filename_by_target_table, configs_by_config_filename, database_variables, \
            project_config, mappings_by_mapping_filename = get_config_structures(config_path, mapping_path, '',
                                                                                 project_config_path)

        config_dependency_graph = get_dependency_graph_for_configs(configs_by_config_filename,
                                                                   mappings_by_mapping_filename,
                                                                   config_filename_by_target_table, project_config,
                                                                   'positive_cases')

        sequencing = {rename_adf_config_activities(entry.config_filename): entry.sequence for entry in
                      project_config.values() if entry.sequence}

        for config_file, sequence in sequencing.items():
            for cf, seq in sequencing.items():
                if sequence >= seq:
                    self.assertNotIn(config_file, config_dependency_graph[cf])

        self.assertIn('test_cycle_direct_cfg_src', config_dependency_graph['test_cycle_direct_cfg_tgt'])

        self.assertIn('test_cycle_indirect_cfg_1', config_dependency_graph['test_cycle_indirect_cfg_2'])
        self.assertIn('test_cycle_indirect_cfg_2', config_dependency_graph['test_cycle_indirect_cfg_3'])
