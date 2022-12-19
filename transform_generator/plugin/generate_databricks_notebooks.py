from itertools import groupby
from os.path import join
from typing import Optional

from transform_generator.generator.databricks_sql_visitor import DataBricksSqlVisitor
from transform_generator.generator.sql_query_generator import SqlQueryGenerator
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.plugin.generate_action import GenerateFilesAction
from transform_generator.project import Project


class GenerateDatabricksNotebooksAction(GenerateFilesAction):
    def generate_files(self, project_group: list[Project], target_dir: str):
        pass

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

    def _notebook_preamble(self, file, project, data_mapping_group, data_mapping) -> Optional[str]:
        return None

    def generate_notebook(self, data_mappings: list[DataMapping], target_output_dir: str):
        with open(join(target_output_dir, data_mappings[0].table_name_qualified + ".sql", "w")) as f:

            queries = [self.generate_query(mapping) for mapping in data_mappings]

            preamble = self._notebook_preamble()
            if preamble:
                f.write(preamble)

            f.write('\n\n-- COMMAND --\n\n'.join(queries))


    def generate_query(self, mapping: DataMapping) -> str:
        def _alias_result_col(ast: TransformExp, column: str):
            if type(ast.result_column) is not AliasedResultCol:
                ast = TransformExp(AliasedResultCol(ast.result_column, column), ast.from_clause, ast.where_clause,
                                   ast.group_by_clause, ast.distinct)
            return ast
        query = SelectQuery()
        table_definition = mapping.table_definition
        cfg_entry = mapping.config_entry

        for key, field in table_definition.non_partitioned_fields.items():
            if key in mapping.target_column_names:
                query.merge_transform_exp(_alias_result_col(mapping.ast(key), key))
            else:
                query.merge_transform_exp(TransformExp(AliasedResultCol(Cast(NullLiteral(), field.data_type), key)))

        for key, field in table_definition.partitioned_fields.items():
            if key in mapping.target_column_names:
                query.merge_transform_exp(_alias_result_col(mapping.ast(key), key))
            else:
                query.merge_transform_exp(TransformExp(AliasedResultCol(Cast(NullLiteral(), field.data_type), key)))

        if mapping.config_entry.target_language.lower() == 'databricks':
            visitor = DataBricksSqlVisitor(cfg_entry.load_type, cfg_entry.project_config,
                                           cfg_entry.config_filename_by_target_table,
                                           mapping.comment_by_target_column_name)
            query.accept(visitor)
            return visitor.sql_string
        else:
            query_generator = SqlQueryGenerator()
            return query_generator.print_select(query)
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

    @staticmethod
    def notebook_name(mapping: DataMapping) -> str:
        return mapping.table_name_qualified()
