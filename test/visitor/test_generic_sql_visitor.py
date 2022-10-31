import unittest

from transform_generator.parser.ast.environment_variable import EnvironmentVariable
from transform_generator.parser.ast.paren_exp import ParenExp
from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.between import Between
from transform_generator.parser.ast.in_clause import InClause
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.case import Case
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.group_by_clause import GroupByClause
from transform_generator.parser.ast.join import Join
from transform_generator.parser.ast.unary_exp import UnaryExp
from transform_generator.parser.ast.null_literal import NullLiteral

from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.window_function_call import WindowFunctionCall
from transform_generator.parser.visitor.generic_sql_visitor import GenericSqlVisitor


def generic_sql_visitor_check(self, expected, ast):
    visitor = GenericSqlVisitor()
    ast.accept(visitor)
    query = visitor.sql_string

    self.assertEqual(query, expected)


class GenericSqlVisitorTestCase(unittest.TestCase):

    def test_aliased_result_col(self):
        ast = AliasedResultCol(Field('field'), 'f')
        expected = 'field AS f'
        generic_sql_visitor_check(self, expected, ast)

    def test_between(self):
        ast = Between(Field('fld'), IntegerLiteral(1), IntegerLiteral(2))
        expected = 'fld BETWEEN 1 AND 2'
        generic_sql_visitor_check(self, expected, ast)

    def test_not_between(self):
        ast = Between(Field('fld'), IntegerLiteral(1), IntegerLiteral(2), True)
        expected = 'fld NOT BETWEEN 1 AND 2'
        generic_sql_visitor_check(self, expected, ast)

    def test_bin_op(self):
        ast = BinOp(IntegerLiteral(4), '+', IntegerLiteral(5))
        expected = "4 + 5"
        generic_sql_visitor_check(self, expected, ast)

    def test_case_searched_single_when(self):
        condition = BinOp(IntegerLiteral(1), '=', IntegerLiteral(1))
        result = StringLiteral('abc')
        when_clauses = [(condition, result)]
        ast = Case(None, when_clauses, None)

        expected = "CASE\nWHEN 1 = 1 THEN 'abc'\nEND"

        generic_sql_visitor_check(self, expected, ast)

    def test_case_searched_two_when_and_else(self):
        condition1 = BinOp(IntegerLiteral(1), '=', IntegerLiteral(1))
        result1 = StringLiteral('abc')

        condition2 = BinOp(IntegerLiteral(2), '=', IntegerLiteral(3))
        result2 = StringLiteral('efg')

        result_else = StringLiteral('xyz')
        when_clauses = [(condition1, result1), (condition2, result2)]
        ast = Case(None, when_clauses, result_else)

        expected = "CASE\nWHEN 1 = 1 THEN 'abc'\nWHEN 2 = 3 THEN 'efg'\nELSE 'xyz'\nEND"

        generic_sql_visitor_check(self, expected, ast)

    def test_case_single_when_else(self):
        test_exp = Field("table_name.field_name")
        when_clause = (StringLiteral('A'), IntegerLiteral(2))
        ast = Case(test_exp, [when_clause], IntegerLiteral(3))
        expected = "CASE table_name.field_name\nWHEN 'A' THEN 2\nELSE 3\nEND"
        generic_sql_visitor_check(self, expected, ast)

    def test_cast(self):
        ast = Cast(StringLiteral('123'), 'INTEGER')
        expected = "CAST('123' AS INTEGER)"
        generic_sql_visitor_check(self, expected, ast)

    def test_field(self):
        ast = Field('field1')
        expected = 'field1'
        generic_sql_visitor_check(self, expected, ast)

    def test_field_key_word_backticks(self):
        ast = Field('LOCATION')
        expected = '`LOCATION`'
        generic_sql_visitor_check(self, expected, ast)

    def test_field_leading_underscore(self):
        ast = Field('_field1')
        expected = '`_field1`'
        generic_sql_visitor_check(self, expected, ast)

    def test_integer_literal(self):
        ast = IntegerLiteral(4)
        expected = '4'
        generic_sql_visitor_check(self, expected, ast)

    def test_negative_unary(self):
        ast = UnaryExp('-', IntegerLiteral(4))
        expected = '- 4'
        generic_sql_visitor_check(self, expected, ast)

    def test_from_clause_no_join(self):
        ast = FromClause('table1')
        expected = 'FROM table1'
        generic_sql_visitor_check(self, expected, ast)

    def test_from_clause_db_no_join(self):
        ast = FromClause('table1', database_name='db')
        expected = 'FROM db.table1'
        generic_sql_visitor_check(self, expected, ast)

    def test_from_clause_alias_no_join(self):
        ast = FromClause('table1', alias='tbl')
        expected = 'FROM table1 AS tbl'
        generic_sql_visitor_check(self, expected, ast)

    def test_from_clause_single_join(self):
        joins = [Join(None, 'tbl2', BinOp(IntegerLiteral(1), "=", IntegerLiteral(1)))]
        ast = FromClause('table1', joins, alias='tbl', )

        expected = 'FROM table1 AS tbl\nJOIN tbl2 ON 1 = 1'
        generic_sql_visitor_check(self, expected, ast)

    def test_from_clause_two_joins(self):
        joins = [Join(None, 'tbl2', BinOp(IntegerLiteral(1), "=", IntegerLiteral(1))),
                 Join(None, 'tbl3', BinOp(IntegerLiteral(1), "=", IntegerLiteral(1)))]
        ast = FromClause('table1', joins, alias='tbl', )

        expected = 'FROM table1 AS tbl\nJOIN tbl2 ON 1 = 1\nJOIN tbl3 ON 1 = 1'
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_no_parameters(self):
        ast = FunctionCall('function_name', [])
        expected = 'function_name()'
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_no_parameters(self):
        ast = FunctionCall('function_name', [StringLiteral('string_parameter')])
        expected = "function_name('string_parameter')"
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_two_parameters(self):
        ast = FunctionCall('function_name', [StringLiteral('string_parameter'), IntegerLiteral(4)])
        expected = "function_name('string_parameter', 4)"
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_three_parameters(self):
        ast = FunctionCall('function_name',
                           [StringLiteral('string_parameter'), IntegerLiteral(4), FunctionCall('inner_function', [])])
        expected = "function_name('string_parameter', 4, inner_function())"
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_nested_function_parameters(self):
        ast = FunctionCall('function_name', [StringLiteral('string_parameter'),
                                             IntegerLiteral(4),
                                             FunctionCall('inner_function', [IntegerLiteral(9)])])
        expected = "function_name('string_parameter', 4, inner_function(9))"
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_distinct(self):
        ast = FunctionCall('count', [Field('field1')], True)
        expected = 'count(DISTINCT field1)'
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_asterisk(self):
        ast = FunctionCall('count', [], False, True)
        expected = 'count(*)'
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_distinct_asterisk(self):
        ast = FunctionCall('count', [], True, True)
        expected = 'count(DISTINCT *)'
        generic_sql_visitor_check(self, expected, ast)

    def test_function_call_asterisk_in_expression(self):
        ast = FunctionCall('count', [BinOp(IntegerLiteral(1), '*', IntegerLiteral(2))])
        expected = 'count(1 * 2)'
        generic_sql_visitor_check(self, expected, ast)

    def test_group_by_one_exp(self):
        ast = GroupByClause([Field("field1")])
        expected = "GROUP BY field1"
        generic_sql_visitor_check(self, expected, ast)

    def test_group_by_three_exp(self):
        ast = GroupByClause([Field('field1'), Field('field2'), Field('field3')])
        expected = 'GROUP BY field1, field2, field3'
        generic_sql_visitor_check(self, expected, ast)

    def test_group_by_having(self):
        having_exp = BinOp(FunctionCall('count', [Field('field1')]), '>', IntegerLiteral(1))
        ast = GroupByClause([Field('field2')], having_exp)
        expected = 'GROUP BY field2 HAVING count(field1) > 1'
        generic_sql_visitor_check(self, expected, ast)

    def test_in_clause(self):
        ast = InClause(Field('field'), [StringLiteral('A')])
        expected = "field IN ('A')"
        generic_sql_visitor_check(self, expected, ast)

    def test_in_clause_multiple(self):
        ast = InClause(Field('field'), [StringLiteral('A'), StringLiteral('B'), StringLiteral('C')])
        expected = "field IN ('A', 'B', 'C')"
        generic_sql_visitor_check(self, expected, ast)

    def test_join_simple_no_condition(self):
        ast = Join(None, 'tbl', None)
        expected = 'JOIN tbl'
        generic_sql_visitor_check(self, expected, ast)

    def test_join_simple_condition(self):
        condition = BinOp(IntegerLiteral(1), '=', IntegerLiteral(2))
        ast = Join(None, 'tbl', condition)
        expected = 'JOIN tbl ON 1 = 2'
        generic_sql_visitor_check(self, expected, ast)

    def test_join_alias(self):
        ast = Join(None, 'tbl', None, 'db', 'alias')
        expected = 'JOIN db.tbl AS alias'
        generic_sql_visitor_check(self, expected, ast)

    def test_join_operator(self):
        ast = Join('LEFT OUTER', 'tbl', None)
        expected = 'LEFT OUTER JOIN tbl'
        generic_sql_visitor_check(self, expected, ast)

    def test_not_unary_exp(self):
        ast = UnaryExp('NOT', BinOp(IntegerLiteral(1), '=', IntegerLiteral(2)))
        expected = 'NOT 1 = 2'
        generic_sql_visitor_check(self, expected, ast)

    def test_not_in_clause(self):
        ast = InClause(Field('field'), [StringLiteral('A')], True)
        expected = "field NOT IN ('A')"
        generic_sql_visitor_check(self, expected, ast)

    def test_null_literal(self):
        ast = NullLiteral()
        expected = 'NULL'
        generic_sql_visitor_check(self, expected, ast)

    def test_paren_exp(self):
        ast = ParenExp(Field('field'))
        expected = '(field)'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_simple(self):
        ast = SelectQuery([Field('field1')])
        expected = 'SELECT\n\tfield1'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_two_result_cols(self):
        ast = SelectQuery([Field('field1'), IntegerLiteral(1)])
        expected = 'SELECT\n\tfield1,\n\t1'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_three_result_cols(self):
        ast = SelectQuery([Field('field1'), IntegerLiteral(1), StringLiteral('abc')])
        expected = "SELECT\n\tfield1,\n\t1,\n\t'abc'"
        generic_sql_visitor_check(self, expected, ast)

    def test_select_distinct(self):
        ast = SelectQuery([Field('field1')], distinct=True)
        expected = 'SELECT DISTINCT\n\tfield1'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_from(self):
        ast = SelectQuery([Field('field1')], from_clause=FromClause('tbl'))
        expected = "SELECT\n\tfield1\nFROM tbl"
        generic_sql_visitor_check(self, expected, ast)

    def test_select_where(self):
        ast = SelectQuery([Field('field1')], where_clause=BinOp(IntegerLiteral(1), '=', IntegerLiteral(2)))
        expected = 'SELECT\n\tfield1\nWHERE 1 = 2'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_where_env_var(self):
        ast = SelectQuery([Field('field1')], where_clause=BinOp(Field('field2'), '=', EnvironmentVariable('processing_date')))
        expected = 'SELECT\n\tfield1\nWHERE field2 = ${processing_date}'
        generic_sql_visitor_check(self, expected, ast)

    def test_select_group_by(self):
        ast = SelectQuery([Field('field1')], group_by_clause=GroupByClause([Field('field1')]))
        expected = 'SELECT\n\tfield1\nGROUP BY field1'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_simple(self):
        ast = WindowFunctionCall(FunctionCall("FIRST", [Field("field", "tbl")]))
        expected = 'FIRST(tbl.field) OVER ()'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_partition_by(self):
        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Field("a"), Field("b", "tbl")]
        ast = WindowFunctionCall(function_call, partition_by)

        expected = 'FIRST(tbl.field) OVER (PARTITION BY a, tbl.b)'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_order_by(self):
        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        order_by = [(Field("a"), None), (Field("b", "tbl"), None)]
        ast = WindowFunctionCall(function_call, [], order_by)

        expected = 'FIRST(tbl.field) OVER (ORDER BY a, tbl.b)'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_order_by_dir(self):
        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        order_by = [(Field("a"), None), (Field("b", "tbl"), "DESC")]
        ast = WindowFunctionCall(function_call, [], order_by)

        expected = 'FIRST(tbl.field) OVER (ORDER BY a, tbl.b DESC)'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_partition_and_order_by(self):
        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Field("a"), Field("b", "tbl")]
        order_by = [(Field("b"), None), (Field("c"), None)]
        ast = WindowFunctionCall(function_call, partition_by, order_by)

        expected = 'FIRST(tbl.field) OVER (PARTITION BY a, tbl.b ORDER BY b, c)'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_partition_by_and_count(self):
        function_call = FunctionCall("COUNT", [], False, True)
        partition_by = [Field("a"), Field("b", "tbl")]
        ast = WindowFunctionCall(function_call, partition_by)

        expected = 'COUNT(*) OVER (PARTITION BY a, tbl.b)'
        generic_sql_visitor_check(self, expected, ast)

    def test_window_func_partition_by_with_case(self):
        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Case(None, [(BinOp(Field("field"), "=", StringLiteral("A")), Field("a"))], Field("b"))]
        ast = WindowFunctionCall(function_call, partition_by)

        expected = "FIRST(tbl.field) OVER (PARTITION BY CASE\nWHEN field = 'A' THEN a\nELSE b\nEND)"
        generic_sql_visitor_check(self, expected, ast)
