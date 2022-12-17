import os
import unittest

from transform_generator.lib.config import get_config_structures, get_new_config_structures
from transform_generator.plugin.loader import get_plugin_loader


def sort_dictionary(dict_to_sort):
    # sorts lists in a dict with lists as values
    for value_list in dict_to_sort.values():
        value_list.sort()
    return dict_to_sort


class TestGetConfig(unittest.TestCase):
    resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Resources')
    positive_cases_folder = os.path.join(resources_folder, 'positive_cases')

    def test_get_config_structures(self):
        config_path = os.path.join(TestGetConfig.positive_cases_folder, 'config')
        mapping_sheet_path = os.path.join(TestGetConfig.positive_cases_folder, 'mapping')
        project_config_path = os.path.join(TestGetConfig.positive_cases_folder, 'project_config',
                                           'project_config_test.csv')
        database_param_prefix = ''
        proj_grp_loader = get_plugin_loader().project_group_loader()
        project_group = proj_grp_loader.load_project_group(project_config_path, TestGetConfig.resources_folder)

        cfg_by_mpg_filename, cfg_filename_by_tgt_tbl, cfg_by_cfg_filename, database_vars, generator_cfg, \
            mpg_by_mpg_filename = get_config_structures(config_path, mapping_sheet_path, database_param_prefix,
                                                        project_config_path)

        new_cfg_by_mpg_filename, new_cfg_filename_by_tgt_tbl, new_cfg_by_cfg_filename, new_database_vars, \
            new_generator_cfg, new_mpg_by_mpg_filename = get_new_config_structures(project_group, database_param_prefix,
                                                                                   project_config_path)

        self.assertEqual(cfg_by_mpg_filename, new_cfg_by_mpg_filename)
        self.assertEqual(cfg_filename_by_tgt_tbl, new_cfg_filename_by_tgt_tbl)
        self.assertEqual(sort_dictionary(cfg_by_cfg_filename), sort_dictionary(new_cfg_by_cfg_filename))
        self.assertEqual(database_vars, new_database_vars)
        self.assertEqual(generator_cfg, new_generator_cfg)
        self.assertEqual(mpg_by_mpg_filename, new_mpg_by_mpg_filename)


if __name__ == '__main__':
    unittest.main()
