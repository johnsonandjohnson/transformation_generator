from itertools import groupby
from os.path import join, split
from typing import Optional
from pathlib import Path

from transform_generator.executor import PipelineStage
from transform_generator.generator.databricks_sql_visitor import DataBricksSqlVisitor
from transform_generator.generator.field import Field
from transform_generator.generator.sql_query_generator import SqlQueryGenerator
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.project import Project


class GenerateDatabricksNotebooksStage(PipelineStage):
    def __init__(self, name: str):
        super().__init__(name)
        self._parallel_db_names_by_db_table_name = None
        self.__file = None
        self._config_filename_by_target_table = None

    def execute(self, projects: list[Project], **kwargs):
        self._config_filename_by_target_table = {m.config_entry.target_table: m.config_entry.key
                                                 for p in projects
                                                 for g in p.data_mapping_groups
                                                 for m in g.data_mappings}
        pass

    def get_db_table_name(self, qualified_table_name: str) -> tuple[str, str]:
        db, table = qualified_table_name.split(".")
        if qualified_table_name in self._parallel_db_names_by_db_table_name:
            return self._parallel_db_names_by_db_table_name[qualified_table_name], table
        return db, table

    def _write(self, data: str):
        self.__file.write(data)

    def _end_cell(self):
        self._write('\n-- COMMAND --\n\n')

    def _notebook_prologue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry) -> Optional[str]:
        pass

    def _notebook_body(self,
                       data_mappings: list[DataMapping],
                       mapping_group_config: ProjectConfigEntry
                       ):
        pass

    def _notebook_epilogue(self,
                           data_mappings: list[DataMapping],
                           mapping_group_config: ProjectConfigEntry):

        pass

    def generate_notebook(self,
                          data_mappings: list[DataMapping],
                          target_file_output_path: str,
                          mapping_group_config: ProjectConfigEntry):
        tgt_dir, tgt_file = split(target_file_output_path)
        tgt_path = Path(tgt_dir)
        if not tgt_path.exists():
            tgt_path.mkdir(parents=True)

        with open(target_file_output_path, "w") as f:
            self.__file = f

            self._notebook_prologue(data_mappings, mapping_group_config)

            self._notebook_body(data_mappings, mapping_group_config)

            self._notebook_epilogue(data_mappings, mapping_group_config)
        self.__file = None

    def generate_query(self,
                       mapping: DataMapping,
                       mapping_group_config: ProjectConfigEntry) -> str:
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
            visitor = DataBricksSqlVisitor(cfg_entry.load_type,
                                           mapping_group_config,
                                           mapping.comment_by_target_column_name)
            query.accept(visitor)
            return visitor.sql_string
        else:
            query_generator = SqlQueryGenerator()
            return query_generator.print_select(query)
        pass

    @staticmethod
    def notebook_name(mapping: DataMapping) -> str:
        return mapping.table_name_qualified


class GenerateTransformationNotebooks(GenerateDatabricksNotebooksStage):
    def execute(self, projects: list[Project], /, *, target_output_dir: str, **kwargs):
        super().execute(projects, target_output_dir=target_output_dir, **kwargs)
        modules = set()

        # Collect all 'parallel_db_names' to create the SQL widgets in the preamble.

        for project in projects:
            for mapping_group in project.data_mapping_groups:
                mapping_group_config = mapping_group.mapping_group_config
                modules.add(mapping_group_config.group_name)

                # We want to process only 'table' type mappings, and to group them by target table name
                # so that we can output all transformations for a given target table into a single notebook.
                table_only_mappings = [m for m in mapping_group.data_mappings
                                       if m.config_entry.target_type.lower() not in {'view', 'program', 'lineage'}]
                sorted_mappings = sorted(table_only_mappings, key=lambda m: m.table_name)
                mappings_by_target_table = groupby(sorted_mappings, key=lambda m: m.table_name)

                for target_table, mappings_iter in mappings_by_target_table:
                    mappings = list(mappings_iter)
                    target_file_path = join(target_output_dir,
                                            project.name,
                                            'databricks',
                                            'generated',
                                            'dml',
                                            mappings[0].table_name_qualified + '.sql')
                    self.generate_notebook(mappings, target_file_path, mapping_group_config)

    def _notebook_body(self,
                       data_mappings: list[DataMapping],
                       mapping_group_config: ProjectConfigEntry
                       ):
        queries = [self.generate_query(mapping, mapping_group_config) for mapping in data_mappings]

        self._write('\n')


class GenerateTableViewCreateNotebooks(GenerateDatabricksNotebooksStage):
    def __init__(self, name: str):
        super().__init__(name)
        self.__type = None

    def execute(self, projects: list[Project], /, target_output_dir: str, **kwargs):
        super().execute(projects, target_output_dir=target_output_dir, **kwargs)
        for project in projects:
            for mapping_group in project.data_mapping_groups:
                tables = [m for m in mapping_group.data_mappings
                          if m.config_entry.target_type.lower() not in {'view', 'lineage'}]
                views = [m for m in mapping_group.data_mappings
                         if m.config_entry.target_type.lower() == 'view']

                tables_by_config_key = groupby(tables, key=lambda m: m.config_entry.key)
                views_by_config_key = groupby(views, key=lambda m: m.config_entry.key)

                for config_key, mappings_iter in views_by_config_key:
                    mappings = list(mappings_iter)
                    if mappings:
                        output_file_path = join(target_output_dir,
                                                project.name,
                                                'databricks',
                                                'generated',
                                                'ddl',
                                                config_key.replace('.csv', '')) + '_views.sql'
                        self.__type = 'view'
                        self.generate_notebook(mappings, output_file_path, mapping_group.mapping_group_config)

                for config_key, mappings_iter in tables_by_config_key:
                    mappings = list(mappings_iter)
                    if mappings:
                        output_file_path = join(target_output_dir,
                                                project.name,
                                                'databricks',
                                                'generated',
                                                'ddl',
                                                config_key.replace('.csv', '')) + '.sql'
                        self.__type = 'table'
                        self.generate_notebook(mappings, output_file_path, mapping_group.mapping_group_config)

    def _notebook_body(self,
                       data_mappings: list[DataMapping],
                       mapping_group_config: ProjectConfigEntry):
        mappings_by_target_name = groupby(data_mappings, key=lambda m: m.table_name)
        first = True
        if self.__type == 'view':
            for target_view, mappings_iter in mappings_by_target_name:
                if not first:
                    self._write('\n')
                    self._end_cell()
                else:
                    first = False
                mappings = list(mappings_iter)
                self._write_ddl_view(mappings, mappings[0].table_definition, mapping_group_config)

        elif self.__type == 'table':
            for target_table, mappings_iter in mappings_by_target_name:
                if not first:
                    self._write('\n')
                    self._end_cell()
                else:
                    first = False
                mappings = list(mappings_iter)
                self._write_ddl_table(mappings[0].table_definition)

    def _write_ddl_view(self,
                        data_mappings: list[DataMapping],
                        table_definition: TableDefinition,
                        mapping_group_config: ProjectConfigEntry):
        db, table = self.get_db_table_name(table_definition.database_name + "." + table_definition.table_name)
        self._write(f'DROP VIEW IF EXISTS {db}.{table};\n\n')
        self._write(f'CREATE VIEW {db}.{table}\n(\n')

        self._write_fields(list(table_definition.partitioned_fields.values()) +
                           list(table_definition.non_partitioned_fields.values()), datatype=False)
        self._write('\n)\n')
        if table_definition.table_description:
            self._write(f'COMMENT {self._sanitize_comment(table_definition.table_description)}\n')
        self._write(' AS ( ')

        queries = [self.generate_query(m, mapping_group_config) for m in data_mappings]

        self._write(' UNION ALL '.join(queries) + ' )\n;')

    def _write_fields(self, fields: list[Field], datatype=True):
        first = True
        for field in fields:
            if not first:
                self._write(',\n')
            else:
                first = False
            self._write(f"\t{field.name}")
            if datatype:
                self._write(f" {field.data_type}")
            if field.column_description:
                self._write(f" COMMENT {self._sanitize_comment(field.column_description)}")

    def _write_ddl_table(self, table_definition: TableDefinition):
        db, table = self.get_db_table_name(table_definition.database_name + "." + table_definition.table_name)

        self._write(f'DROP TABLE IF EXISTS {db}.{table};\n\n')

        self._write(f'CREATE TABLE {db}.{table}\n(\n')
        self._write_fields(table_definition.non_partitioned_fields.values())
        self._write('\n)\n')

        if table_definition.partitioned_fields:
            self._write('PARTITIONED BY \n(\n')
            self._write_fields(table_definition.partitioned_fields.values())
            self._write('\n)\n')

        if table_definition.table_description:
            self._write(f"COMMENT {self._sanitize_comment(table_definition.table_description)}")

        self._write('\nSTORED AS PARQUET\n' +
                    f"LOCATION '/mnt/dct/chdp/{table_definition.database_name.lower()}" +
                    f"/{table_definition.table_name.lower()}';")

        if table_definition.partitioned_fields:
            self._write(f'\n\nALTER TABLE {db}.{table} RECOVER PARTITIONS;')

    def _sanitize_comment(self, comment: str):
        """
        This comment standardizes comment strings
        @param comment: String of the comment
        @return: An updated comment with the appropriate length of characters and formatting
        """
        if comment == '':
            return comment
        # Note: 256-character limit set here
        sanitized_comment = comment[0:255].replace("'", r"\'")

        return f"'{sanitized_comment}'"
