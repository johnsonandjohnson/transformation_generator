from abc import ABC

from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.unary_exp import UnaryExp

from ..ast.between import Between
from ..ast.bin_op import BinOp
from ..ast.case import Case
from ..ast.cast import Cast
from ..ast.decimal_literal import DecimalLiteral
from ..ast.environment_variable import EnvironmentVariable
from ..ast.field import Field
from ..ast.from_clause import FromClause
from ..ast.function_call import FunctionCall
from ..ast.in_clause import InClause
from ..ast.join import Join
from ..ast.null_literal import NullLiteral
from ..ast.integer_literal import IntegerLiteral
from ..ast.paren_exp import ParenExp
from ..ast.select_query import SelectQuery
from ..ast.string_literal import StringLiteral
from ..ast.transform_exp import TransformExp
from ..ast.window_function_call import WindowFunctionCall
from ..ast.group_by_clause import GroupByClause


class AstVisitor(ABC):
    def visit_aliased_result_col(self, aliased_result_col: AliasedResultCol):
        aliased_result_col.exp.accept(self)

    def visit_between(self, between: Between):
        between.test_exp.accept(self)
        between.begin_exp.accept(self)
        between.end_exp.accept(self)

    def visit_bin_op(self, bin_op: BinOp):
        bin_op.left.accept(self)
        bin_op.right.accept(self)

    def visit_case(self, case: Case):
        if case.exp is not None:
            case.exp.accept(self)

        for condition, result_exp in case.when_clauses:
            condition.accept(self)
            result_exp.accept(self)

        if case.else_clause is not None:
            case.else_clause.accept(self)

    def visit_cast(self, cast: Cast):
        cast.parameter.accept(self)

    def visit_decimal_literal(self, decimal_literal: DecimalLiteral):
        pass

    def visit_environment_variable(self, environment_variable: EnvironmentVariable):
        pass

    def visit_field(self, field: Field):
        pass

    def visit_from_clause(self, from_clause: FromClause):
        for join in from_clause.joins:
            join.accept(self)

    def visit_function_call(self, function_call: FunctionCall):
        for parameter in function_call.parameters:
            parameter.accept(self)

    def visit_group_by_clause(self, group_by_clause: GroupByClause):
        for exp in group_by_clause.exp_list:
            exp.accept(self)

        if group_by_clause.having_exp:
            group_by_clause.having_exp.accept(self)

    def visit_in_clause(self, in_clause: InClause):
        in_clause.exp.accept(self)

        for exp in in_clause.exp_list:
            exp.accept(self)

    def visit_join(self, join: Join):
        if join.condition is not None:
            join.condition.accept(self)

    def visit_unary_exp(self, unary_exp: UnaryExp):
        unary_exp.exp.accept(self)

    def visit_null_literal(self, null_literal: NullLiteral):
        pass

    def visit_integer_literal(self, integer_literal: IntegerLiteral):
        pass

    def visit_paren_exp(self, paren_exp: ParenExp):
        paren_exp.exp.accept(self)

    def visit_select_query(self, select_query: SelectQuery):
        for col in select_query.result_columns:
            col.accept(self)
        if select_query.from_clause is not None:
            select_query.from_clause.accept(self)

        if select_query.where_clause is not None:
            select_query.where_clause.accept(self)

        if select_query.group_by_clause is not None:
            select_query.group_by_clause.accept(self)

    def visit_string_literal(self, string_literal: StringLiteral):
        pass

    def visit_transform_exp(self, transform_exp: TransformExp):
        transform_exp.result_column.accept(self)

        if transform_exp.from_clause is not None:
            transform_exp.from_clause.accept(self)

        if transform_exp.where_clause is not None:
            transform_exp.where_clause.accept(self)

        if transform_exp.group_by_clause is not None:
            transform_exp.group_by_clause.accept(self)

    def visit_window_function_call(self, window_function_call: WindowFunctionCall):
        for field in window_function_call.partition_by:
            field.accept(self)

        for field in window_function_call.order_by:
            field.accept(self)
