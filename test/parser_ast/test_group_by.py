from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.group_by_clause import GroupByClause
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.integer_literal import IntegerLiteral

import unittest


class GroupByTestCase(unittest.TestCase):
    def test_single_field(self):
        expected_str = "SUM(tbl.field) FROM tbl GROUP BY tbl.field2"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field", "tbl")
        expected_ast = TransformExp(FunctionCall("SUM", [field]), FromClause("tbl"), None, GroupByClause([Field("Field2", "tbl")]))
        self.assertEqual(expected_ast, ast)

    def test_multiple_fields(self):
        expected_str = "SUM(tbl.field) FROM tbl GROUP BY tbl.field2, tbl.field3"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field", "tbl")
        group_by = GroupByClause([Field("field2", "tbl"), Field("field3", "tbl")])
        expected_ast = TransformExp(FunctionCall("SUM", [field]), FromClause("tbl"), None, group_by)
        self.assertEqual(expected_ast, ast)

    def test_cast(self):
        expected_str = "SUM(tbl.field) FROM tbl GROUP BY CAST('abc' AS STRING)"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field", "tbl")
        group_by = GroupByClause([Cast(StringLiteral('abc'), "STRING")])
        expected_ast = TransformExp(FunctionCall("SUM", [field]), FromClause("tbl"), None, group_by)
        self.assertEqual(expected_ast, ast)

    def test_having_clause(self):
        expected_str = "SUM(tbl.field) FROM tbl GROUP BY tbl.field HAVING min(tbl.field) >= 0"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field", "tbl")
        having_exp = BinOp(FunctionCall("min", [field]), ">=", IntegerLiteral(0))
        expected_ast = TransformExp(FunctionCall("SUM", [field]), FromClause("tbl"), None, GroupByClause([field], having_exp))
        self.assertEqual(expected_ast, ast)

