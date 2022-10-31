from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.bin_op import BinOp

import unittest


class WhereTest(unittest.TestCase):
    def test_simple_where(self):
        expected_str = "tbl.field FROM tbl WHERE 1 = 1"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(str(ast), expected_str)
        bin_op = BinOp(IntegerLiteral(1), "=", IntegerLiteral(1))
        expected = TransformExp(Field("field", "tbl"), FromClause("tbl"), bin_op)
        self.assertEqual(ast, expected)
