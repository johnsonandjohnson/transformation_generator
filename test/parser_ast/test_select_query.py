from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.join import Join
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.integer_literal import IntegerLiteral

import unittest


class SqlQueryTestCase(unittest.TestCase):

    def test_single_exp(self):
        result_col = Field("field1", "tbl")
        from_clause = FromClause("tbl", [Join(None, "tbl2",
                                              BinOp(Field("id", "tbl"), "=", Field("id", "tbl2")), None)])
        exp = TransformExp(result_col, from_clause)

        query = SelectQuery()
        query.merge_transform_exp(exp)

        self.assertEqual(query.result_columns, [result_col])
        self.assertEqual(query.from_clause, from_clause)

    def test_two_join_exp(self):
        result_col1 = Field("field1", "tbl")
        from_clause1 = FromClause("tbl", [Join(None, "tbl2",
                                               BinOp(Field("id", "tbl"), "=", Field("id", "tbl2")), None)])
        exp1 = TransformExp(result_col1, from_clause1, distinct=False)

        query = SelectQuery()
        query.merge_transform_exp(exp1)

        result_col2 = Field("field1", "tbl")
        from_clause2 = FromClause("tbl", [Join(None, "tbl3",
                                               BinOp(Field("id", "tbl"), "=", Field("id", "tbl3")), None)])
        exp2 = TransformExp(result_col2, from_clause2, distinct=True)

        query.merge_transform_exp(exp2)

        exp_from_clause = FromClause("tbl", [from_clause1.joins[0], from_clause2.joins[0]])

        self.assertEqual(query.result_columns, [result_col1, result_col2])
        self.assertEqual(query.from_clause, exp_from_clause)
        self.assertEqual(query.distinct, True)

    def test_two_identical_joins(self):
        result_col1 = Field("field1", "tbl")
        from_clause = FromClause("tbl", [Join(None, "tbl2",
                                              BinOp(Field("id", "tbl"), "=", Field("id", "tbl2")), None)])
        exp1 = TransformExp(result_col1, from_clause)

        query = SelectQuery()
        query.merge_transform_exp(exp1)

        result_col2 = Field("field2", "tbl2")
        query.merge_transform_exp(TransformExp(result_col2, from_clause))

        self.assertEqual(query.result_columns, [result_col1, result_col2])
        self.assertEqual(query.from_clause, from_clause)

    def test_where_clause(self):
        result_col = Field("field1", "tbl")
        from_clause = FromClause("tbl")
        where_clause = BinOp(IntegerLiteral(1), "=", IntegerLiteral(1))
        exp = TransformExp(result_col, from_clause, where_clause)

        query = SelectQuery()
        query.merge_transform_exp(exp)

        self.assertEqual(query.where_clause, where_clause)
