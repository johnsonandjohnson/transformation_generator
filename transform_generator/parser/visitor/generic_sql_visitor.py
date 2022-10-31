from transform_generator.parser.ast.paren_exp import ParenExp
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.between import Between
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.case import Case
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.group_by_clause import GroupByClause
from transform_generator.parser.ast.in_clause import InClause
from transform_generator.parser.ast.join import Join
from transform_generator.parser.ast.unary_exp import UnaryExp
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.window_function_call import WindowFunctionCall
from transform_generator.parser.keywords import keywords

from .ast_visitor import AstVisitor
from ..ast.decimal_literal import DecimalLiteral
from ..ast.environment_variable import EnvironmentVariable


class GenericSqlVisitor(AstVisitor):
    def __init__(self):
        self.clear()

    def clear(self):
        self._indent_level = 0
        self._sql_string = ""
        self._begin_line = True

    @property
    def sql_string(self):
        return self._sql_string

    def _indent(self):
        self._indent_level += 1

    def _unindent(self):
        self._indent_level -= 1

    def _emit(self, output_str):
        if self._begin_line:
            for x in range(0, self._indent_level):
                self._sql_string += "\t"
            self._begin_line = False

        self._sql_string += output_str

    def _end_line(self):
        self._sql_string += "\n"
        self._begin_line = True

    def _get_database_name(self, database_name: str, table_name: str):
        database_table_name = ""
        if database_name:
            database_table_name = database_name + "."
        if table_name[0] == '_':
            table_name = '`' + table_name + '`'
        database_table_name += table_name
        return database_table_name

    def visit_aliased_result_col(self, aliased_result_col: AliasedResultCol):
        aliased_result_col.exp.accept(self)

        if aliased_result_col.alias:
            self._emit(" AS " + aliased_result_col.alias)

    def visit_between(self, between: Between):
        between.test_exp.accept(self)

        if between.not_modifier:
            self._emit(" NOT")
        self._emit(" BETWEEN ")
        between.begin_exp.accept(self)
        self._emit(" AND ")
        between.end_exp.accept(self)

    def visit_bin_op(self, bin_op: BinOp):
        bin_op.left.accept(self)
        self._emit(" " + bin_op.op + " ")
        bin_op.right.accept(self)

    def visit_case(self, case: Case):
        self._emit("CASE")
        if case.exp:
            self._emit(" ")
            case.exp.accept(self)
        self._end_line()

        for condition, result_exp in case.when_clauses:
            self._emit("WHEN ")
            condition.accept(self)
            self._emit(" THEN ")
            result_exp.accept(self)
            self._end_line()

        if case.else_clause:
            self._emit("ELSE ")
            case.else_clause.accept(self)
            self._end_line()

        self._emit("END")

    def visit_cast(self, cast: Cast):
        self._emit("CAST(")
        cast.parameter.accept(self)
        self._emit(" AS " + cast.data_type + ")")

    def visit_decimal_literal(self, decimal_literal: DecimalLiteral):
        self._emit(str(decimal_literal.value))

    def visit_environment_variable(self, environment_variable: EnvironmentVariable):
        self._emit('${')
        self._emit(environment_variable.env_var)
        self._emit('}')

    def visit_field(self, field: Field):
        if field.field_name.lower() in keywords() or field.field_name[0] == '_':
            result = '`' + field.field_name + '`'
        else:
            result = field.field_name
        if field.table_name is not None:
            result = "`" + field.table_name + "`" if field.table_name[0] == "_" else field.table_name + "." + result
        self._emit(result)

    def visit_from_clause(self, from_clause: FromClause):
        database_table_name = self._get_database_name(from_clause.database_name, from_clause.table_name)

        self._emit("FROM " + database_table_name)

        if from_clause.alias:
            self._emit(" AS " + from_clause.alias)

        for join in from_clause.joins:
            self._end_line()
            join.accept(self)

    def visit_function_call(self, function_call: FunctionCall):
        self._emit(function_call.name + "(")
        if function_call.distinct:
            self._emit("DISTINCT ")
        if function_call.asterisk:
            self._emit("*")
        elif function_call.parameters:
            count = 0
            for param in function_call.parameters:
                if count > 0:
                    self._emit(", ")
                param.accept(self)
                count += 1
        self._emit(")")

    def visit_group_by_clause(self, group_by_clause: GroupByClause):
        self._emit("GROUP BY ")

        count = 0
        for exp in group_by_clause.exp_list:
            if count > 0:
                self._emit(", ")
            exp.accept(self)
            count += 1

        if group_by_clause.having_exp:
            self._emit(" HAVING ")
            group_by_clause.having_exp.accept(self)

    def visit_in_clause(self, in_clause: InClause):
        self._emit(str(in_clause.exp) + " ")
        if in_clause.not_in:
            self._emit("NOT ")
        self._emit("IN (" + ", ".join([str(exp) for exp in in_clause.exp_list]) + ")")

    def visit_integer_literal(self, integer_literal: IntegerLiteral):
        self._emit(str(integer_literal.value))

    def visit_join(self, join: Join):
        database_table_name = self._get_database_name(join.database_name, join.table_name)

        if join.operator:
            self._emit(join.operator + " ")
        self._emit("JOIN ")

        self._emit(database_table_name)

        if join.alias:
            self._emit(" AS " + join.alias)

        if join.condition:
            self._emit(" ON ")
            join.condition.accept(self)

    def visit_not_exp(self, unary_exp: UnaryExp):
        self._emit(unary_exp.op + ' ')
        unary_exp.exp.accept(self)

    def visit_null_literal(self, null_literal: NullLiteral):
        self._emit(str(null_literal))

    def visit_paren_exp(self, paren_exp: ParenExp):
        self._emit('(')
        paren_exp.exp.accept(self)
        self._emit(')')

    def _after_where_clause(self, select_query):
        pass

    def _no_where_clause(self, select_query):
        pass

    def visit_select_query(self, select_query: SelectQuery):
        self._emit("SELECT")

        if select_query.distinct:
            self._emit(" DISTINCT")
        self._indent()

        count = 0
        for col in select_query.result_columns:
            if count > 0:
                self._emit(",")
            self._end_line()
            col.accept(self)
            count += 1

        self._unindent()

        if select_query.from_clause:
            self._end_line()
            select_query.from_clause.accept(self)

        if select_query.where_clause:
            self._end_line()
            self._emit("WHERE ")
            select_query.where_clause.accept(self)
            self._after_where_clause(select_query)
        else:
            self._no_where_clause(select_query)

        if select_query.group_by_clause:
            self._end_line()
            select_query.group_by_clause.accept(self)

    def visit_string_literal(self, string_literal: StringLiteral):
        self._emit("'" + string_literal.value + "'")

    def visit_transform_exp(self, transform_exp: TransformExp):
        super().visit_transform_exp(transform_exp)

    def visit_window_function_call(self, window_function_call: WindowFunctionCall):
        window_function_call.function_call.accept(self)
        self._emit(" OVER (")

        if len(window_function_call.partition_by) > 0:
            self._emit("PARTITION BY ")
            count = 0
            for exp in window_function_call.partition_by:
                if count > 0:
                    self._emit(", ")
                exp.accept(self)
                count += 1

        if len(window_function_call.order_by) > 0:
            if len(window_function_call.partition_by) > 0:
                self._emit(" ")
            self._emit("ORDER BY ")
            count = 0
            for exp, direction in window_function_call.order_by:
                if count > 0:
                    self._emit(", ")
                exp.accept(self)
                if direction is not None:
                    self._emit(" " + direction)
                count += 1
        self._emit(")")
