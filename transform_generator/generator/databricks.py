from itertools import groupby

from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.project import Project


class DataBricksNotebookGenerator:

    def generate_notebooks(self, projects: list[Project]):
        modules = set()
        for project in projects:
            for mapping_group in project.data_mapping_groups:
                mapping_group_config = mapping_group.mapping_group_config
                modules.add(mapping_group_config.group_name)

                # We want to process only 'table' type mappings, and to group them by target table name
                # so that we can output all transformations for a given target table into a single notebook.
                table_only_mappings = (m for m in mapping_group.data_mappings
                                       if m.config_entry.target_type.lower() == 'table')
                sorted_mappings = sorted(table_only_mappings, key=lambda m: m.table_name)
                mappings_by_target_table = groupby(sorted_mappings, key=lambda m: m.table_name)

                for target_table, mapping in mappings_by_target_table:
                    self.generate_notebook(mapping)

    def _notebook_preamble(self, file, project, data_mapping_group, data_mapping):
        pass

    def generate_notebook(self, data_mappings: list[DataMapping]):
        pass
        self._notebook_preamble()
        # Open file with appropriate file name

        # Generate widgets, preambles, etc, based on config (config is on data mapping object)

        # generate any footers.

    def generate_query(self, mapping: DataMapping) -> str:
        pass

    def create_sql_widget(self, project_config: dict[str, ProjectConfigEntry]) -> str:
        """
        This function takes in the generator config file, loops through it, and extracts the variables that need to be
        created as widgets for the database variables.
        @param project_config: A dictionary with the config filename as the key and generator config entries as the values
        @return: a string containing the widget text for the beginning of the sql files
        """
        widgets = set()
        widgets.add('CREATE WIDGET TEXT processing_date DEFAULT \'\';')
        for config_filename, project_config_entry in project_config.items():
            parallel_db_name = project_config_entry.parallel_db_name
            if project_config_entry.parallel_db_name != '':
                widgets.add('CREATE WIDGET TEXT ' + parallel_db_name + '_db DEFAULT \'' + parallel_db_name + '\';')
        widget_string = '\n'.join(widgets)
        return widget_string
