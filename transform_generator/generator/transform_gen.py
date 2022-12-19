from typing import Dict

from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.transform_exp import TransformExp

from transform_generator.generator.databricks_sql_visitor import DataBricksSqlVisitor
from transform_generator.generator.sql_query_generator import SqlQueryGenerator





def generate_select_query(data_mapping: DataMapping, table_definition: TableDefinition, language: str = 'DATABRICKS',
                          project_config: Dict[str, ProjectConfigEntry] = {},
                          config_filename_by_target_table: Dict[str, str] = {}, load_type: str = 'full') -> str:
    """
    This creates a select query based on input variables and returns it as a string
    @param data_mapping: The business object to convert from.
    @param table_definition: table definition generated based on schema file
    @param language: Optional -- string of the target language, defaults to 'DATABRICKS'
    @param project_config: Optional -- A dictionary with the config filename as the key and generator config entries
        as the values, defaults to {}
    @param config_filename_by_target_table: Optional -- A dictionary with the target table as the key and the config
        filename as the values, defaults to {}
    @param load_type: Optional -- string of the load type (i.e. full or incremental), defaults to 'full'
    @return: A string of the generated select query
    """
    query = SelectQuery()
    for key, field in table_definition.non_partitioned_fields.items():
        if key in data_mapping.target_column_names:
            query.merge_transform_exp(_alias_result_col(data_mapping.ast(key), key))
        else:
            query.merge_transform_exp(TransformExp(AliasedResultCol(Cast(NullLiteral(), field.data_type), key)))

    for key, field in table_definition.partitioned_fields.items():
        if key in data_mapping.target_column_names:
            query.merge_transform_exp(_alias_result_col(data_mapping.ast(key), key))
        else:
            query.merge_transform_exp(TransformExp(AliasedResultCol(Cast(NullLiteral(), field.data_type), key)))

    if language.lower() == 'databricks':
        visitor = DataBricksSqlVisitor(load_type, project_config, config_filename_by_target_table,
                                       data_mapping.comment_by_target_column_name)
        query.accept(visitor)
        return visitor.sql_string
    else:
        query_generator = SqlQueryGenerator()
        return query_generator.print_select(query)
