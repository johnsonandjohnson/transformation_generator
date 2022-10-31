from transform_generator.parser.ast.select_query import SelectQuery
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.join import Join
from transform_generator.parser.ast.bin_op import BinOp
import unittest

from transform_generator.generator.sql_query_generator import SqlQueryGenerator


class TestSqlQueryGenerator(unittest.TestCase):

    def test_simple_select(self):
        exp = TransformExp(Field("field1", "tbl"), FromClause("tbl", []))
        qry = SelectQuery()
        qry.merge_transform_exp(exp)

        gen = SqlQueryGenerator()

        str = gen.print_select(qry)

        expected = """SELECT tbl.field1\nFROM tbl"""
        self.assertEqual(str, expected)

    def test_simple_join(self):
        join = Join(None, "tbl2", BinOp(Field("field1", "tbl"), "=", Field("field2", "tbl2")), None)
        exp = TransformExp(Field("field1", "tbl"), FromClause("tbl", [join]))
        qry = SelectQuery()
        qry.merge_transform_exp(exp)

        gen = SqlQueryGenerator()

        str = gen.print_select(qry)

        expected = """SELECT tbl.field1\nFROM tbl\n\tJOIN tbl2 ON tbl.field1 = tbl2.field2"""
        self.assertEqual(str, expected)

    def test_multiple_columns(self):
        col1 = Field("field1", "tbl")
        col2 = Field("field2", "tbl")
        col3 = Field("field3", "tbl")
        from_clause = FromClause("tbl", [])
        qry = SelectQuery()
        qry.merge_transform_exp(TransformExp(col1, from_clause))
        qry.merge_transform_exp(TransformExp(col2, from_clause))
        qry.merge_transform_exp(TransformExp(col3, from_clause))

        gen = SqlQueryGenerator()

        str = gen.print_select(qry)

        expected = """SELECT tbl.field1,\n\ttbl.field2,\n\ttbl.field3\nFROM tbl"""
        self.assertEqual(str, expected)
