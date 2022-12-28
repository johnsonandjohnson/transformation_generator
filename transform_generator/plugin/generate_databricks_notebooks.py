from itertools import groupby
from os.path import join
from typing import Optional

from transform_generator.data_mapping_group import DataMappingGroup
from transform_generator.generator.databricks_sql_visitor import DataBricksSqlVisitor
from transform_generator.generator.sql_query_generator import SqlQueryGenerator
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.plugin.generate_action import GenerateFilesAction
from transform_generator.project import Project


class GenerateDatabricksNotebooksAction(GenerateFilesAction):
    def __init__(self):
        self._parallel_db_names_by_db_table_name = None

    def get_db_table_name(self, qualified_table_name: str) -> tuple[str, str]:
        db, table = qualified_table_name.split(".")
        if qualified_table_name in self._parallel_db_names_by_db_table_name:
            return self._parallel_db_names_by_db_table_name[qualified_table_name], table
        return db, table

    def generate_files(self, projects: list[Project], target_output_dir: str):
        modules = set()

        # Collect all 'parallel_db_names' to create the SQL widgets in the preamble.
        self._parallel_db_names_by_db_table_name = {m.table_name_qualified: g.mapping_group_config.parallel_db_name
                                                    for p in projects
                                                    for g in p.data_mapping_groups
                                                    for m in g.data_mappings
                                                    if g.mapping_group_config.parallel_db_name}
        for project in projects:
            for mapping_group in project.data_mapping_groups:
                mapping_group_config = mapping_group.mapping_group_config
                modules.add(mapping_group_config.group_name)

                # We want to process only 'table' type mappings, and to group them by target table name
                # so that we can output all transformations for a given target table into a single notebook.
                table_only_mappings = (m for m in mapping_group.data_mappings
                                       if m.config_entry.target_type.lower() not in {'view', 'program', 'lineage'})
                sorted_mappings = sorted(table_only_mappings, key=lambda m: m.table_name)
                mappings_by_target_table = groupby(sorted_mappings, key=lambda m: m.table_name)

                for target_table, mappings in mappings_by_target_table:
                    self.generate_notebook(list(mappings), target_output_dir, mapping_group_config)

    def _notebook_prologue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry) -> Optional[str]:
        preamble = ""
        if mapping_group_config.notebooks_to_execute:
            preamble += '%run\n' + '\n'.join(mapping_group_config.notebooks_to_execute.split(','))

        if self._parallel_db_names_by_db_table_name.values():
            if preamble:
                preamble += '\n'
            return self.create_sql_widget(sorted(self._parallel_db_names_by_db_table_name.values()))
        return preamble

    def _notebook_epilogue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry):
        load_synapse = any(True for d in data_mappings if d.config_entry.load_synapse == 'Y')
        folder_path = ''
        if mapping_group_config.group_name:
            folder_path += '/' + mapping_group_config.group_name

        if load_synapse:
            db, table = self.get_db_table_name(data_mappings[0].table_name_qualified)
            return self.create_synapse_execution(folder_path, db, table)
        return None

    def generate_notebook(self,
                          data_mappings: list[DataMapping],
                          target_output_dir: str,
                          mapping_group_config: ProjectConfigEntry):
        with open(join(target_output_dir, data_mappings[0].table_name_qualified + ".sql"), "w") as f:
            queries = [self.generate_query(mapping) for mapping in data_mappings]

            preamble = self._notebook_prologue()
            if preamble:
                f.write(preamble)

            f.write('\n\n-- COMMAND --\n\n'.join(queries))

            footer = self._notebook_epilogue()
            if footer:
                f.write(footer)

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

    def create_sql_widget(self, parallel_db_names: list[str]) -> str:
        """
        This function takes in the generator config file, loops through it, and extracts the variables that need to be
        created as widgets for the database variables.
        @param project_config: A dictionary with the config filename as the key and generator config entries as the values
        @return: a string containing the widget text for the beginning of the sql files
        """
        widgets = set()
        widgets.add('CREATE WIDGET TEXT processing_date DEFAULT \'\';')

        for parallel_db_name in parallel_db_names:
                widgets.add('CREATE WIDGET TEXT ' + parallel_db_name + '_db DEFAULT \'' + parallel_db_name + '\';')
        widget_string = '\n'.join(widgets)
        return widget_string

    def create_synapse_execution(self, folder_location: str, database_name: str, table_name: str):
        return f'%run {folder_location}/cons_digital_core/databricks/synapse/syn_load ' \
               f'$src_table = "{table_name}" $tgt_table = "{table_name}" $tgt_schema = ' \
               f'{database_name}'

    @staticmethod
    def notebook_name(mapping: DataMapping) -> str:
        return mapping.table_name_qualified
