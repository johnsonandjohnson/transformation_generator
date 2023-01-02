from typing import Optional

from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.sql_scripts import get_db_table_name
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.case import Case
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.visitor.generic_sql_visitor import GenericSqlVisitor


class DataBricksSqlVisitor(GenericSqlVisitor):
    def __init__(self,
                 load_type,
                 mapping_group_config_entry: ProjectConfigEntry=None,
                 comment_by_target_column_name=dict[str, Optional[str]]):
        """
        Initializes a DataBricksSqlVisitor
        @param load_type: string of the load type (ex: full or incremental)
        @param project_config: Optional, dictionary with the config filename as the key and generator config entries
            as the values
        @param config_filename_by_target_table: Optional, dictionary with target table as the key and config filename as
            the value
        @param comment_by_target_column_name: dictionary with target table as key and comment from mapping sheet file as value
        note: project_config and config_filename_by_target_table are both optional because they are only used to
            determine the database name when using parallel environments, but are unnecessary in a simplified sql query
            creation
        """
        super().__init__()
        self._load_type = load_type
        self._mapping_group_config_entry = mapping_group_config_entry
        self._inside_case_rewrite = False
        self._inside_collect_list_function_rewrite = False
        self._inside_isnull_function_rewrite = False
        self._comment_by_target_column_name = comment_by_target_column_name

    @staticmethod
    def _is_trim_or_field(exp):
        """
        Determine if a particular expression is either a TRIM function or a field reference.
        :param exp: The expression to test.
        :return: A boolean indicating if the function is either a TRIM function call or a field reference.
        """
        return (isinstance(exp, FunctionCall) and exp.name.lower() == 'trim') or isinstance(exp, Field)

    def _is_rewrite_cast_function(self, exp):
        """
        Analyzes an expression to determine if it is subject to the CAST re-write for timestamp.
        We look to see if a function is 'concatenate' with 3 parameters: a field, a space, followed by a field.
        :param exp: The expression to test.
        :return: A boolean indicating if the function is subject to being re-written.
        """
        return self._is_trim_or_field(exp) or (
                isinstance(exp, FunctionCall) and exp.name.lower() == 'concat'
                and len(exp.parameters) == 3 and self._is_trim_or_field(exp.parameters[0])
                and exp.parameters[1] == StringLiteral(" ")
                and self._is_trim_or_field(exp.parameters[2]))

    @staticmethod
    def _get_year_substitution(date_exp):
        year = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(1), IntegerLiteral(4)])
        month = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(5), IntegerLiteral(2)])
        day = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(7), IntegerLiteral(2)])
        dash = StringLiteral('-')
        concat = FunctionCall('CONCAT', [year, dash, month, dash, day])
        return concat

    @staticmethod
    def _get_date_time_substitution(date_exp):
        hour = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(1), IntegerLiteral(2)])
        min = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(3), IntegerLiteral(2)])
        second = FunctionCall('SUBSTR', [date_exp, IntegerLiteral(5), IntegerLiteral(2)])
        colon = StringLiteral(':')
        year_concat = DataBricksSqlVisitor._get_year_substitution(date_exp)
        time_concat = FunctionCall('CONCAT', [hour, colon, min, colon, second])
        concat = FunctionCall('CONCAT', [year_concat, StringLiteral(' '), time_concat])
        return concat

    @staticmethod
    def _get_rewritten_cast(date_exp):
        date_test = BinOp(FunctionCall('regexp_extract', [date_exp, StringLiteral('^[0-9]{8}$'), IntegerLiteral(0)]),
                          '<>', StringLiteral(''))

        time_test = BinOp(
            FunctionCall('regexp_extract', [date_exp, StringLiteral('^[0-9]{8} [0-9]{6}$'), IntegerLiteral(0)]),
            '<>', StringLiteral(''))

        date_cast = Cast(DataBricksSqlVisitor._get_year_substitution(date_exp), 'TIMESTAMP')
        date_time_cast = Cast(DataBricksSqlVisitor._get_date_time_substitution(date_exp), 'TIMESTAMP')

        date_when = (date_test, date_cast)
        time_when = (time_test, date_time_cast)

        else_cast = Cast(date_exp, 'TIMESTAMP')
        return Case(None, [date_when, time_when], else_cast)

    def visit_cast(self, cast: Cast):
        if cast.data_type.lower() == 'timestamp' and self._is_rewrite_cast_function(cast.parameter) and \
                not self._inside_case_rewrite:
            rewritten_cast = self._get_rewritten_cast(cast.parameter)
            self._inside_case_rewrite = True
            rewritten_cast.accept(self)
            self._inside_case_rewrite = False
        else:
            super().visit_cast(cast)

    @staticmethod
    def _rewrite_isnull(is_null: FunctionCall):
        return FunctionCall('NVL', is_null.parameters)

    def visit_function_call(self, function_call: FunctionCall):
        if not self._inside_isnull_function_rewrite and function_call.name.lower() == 'isnull':
            rewritten_isnull = self._rewrite_isnull(function_call)
            self._inside_isnull_function_rewrite = True
            rewritten_isnull.accept(self)
            self._inside_isnull_function_rewrite = False
        else:
            super().visit_function_call(function_call)

    def _get_database_name(self, database_name: str, table_name: str) -> str:
        if self._mapping_group_config_entry.parallel_db_name != '':
            database_name = '${' + self._mapping_group_config_entry.parallel_db_name + '_db}'
        return super()._get_database_name(database_name, table_name)

    def _after_where_clause(self, select_query):
        if self._load_type == 'incremental':
            self._emit(' AND ' + select_query.from_clause.table_name
                       + '._UPT_ > (SELECT LAST_REFRESHED FROM SC_P_CON_CORE.CDC_AUDIT WHERE ENTITY="'
                       + select_query.from_clause.database_name + '.' + select_query.from_clause.table_name + '")')

    def _no_where_clause(self, select_query):
        if self._load_type == 'incremental':
            self._emit(' WHERE ' + select_query.from_clause.table_name
                       + '._UPT_ > (SELECT LAST_REFRESHED FROM SC_P_CON_CORE.CDC_AUDIT WHERE ENTITY="'
                       + select_query.from_clause.database_name + '.' + select_query.from_clause.table_name + '")')

    def visit_aliased_result_col(self, aliased_result_col: AliasedResultCol):
        super().visit_aliased_result_col(aliased_result_col)
        comment = self._comment_by_target_column_name.get(aliased_result_col.alias, None)
        if comment:
            self._emit("\t-- " + comment)
