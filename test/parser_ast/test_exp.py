from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.between import Between

import unittest


class ExpTestCase(unittest.TestCase):
    @staticmethod
    def test_arithmetic_exp():
        ast = get_ast_for_str("2 + 3 * 5", "exp")

        assert ast == BinOp(IntegerLiteral(2), "+", BinOp(IntegerLiteral(3), "*", IntegerLiteral(5)))

    def test_and_or_exp(self,):
        ast = get_ast_for_str("1 < 2 AND 2 > 3 OR 'ABC' <> 'XYZ'", "exp")

        first_compare = BinOp(IntegerLiteral(1), "<", IntegerLiteral(2))
        second_compare = BinOp(IntegerLiteral(2), ">", IntegerLiteral(3))
        third_compare = BinOp(StringLiteral("ABC"), "<>", StringLiteral("XYZ"))
        and_op = BinOp(first_compare, "AND", second_compare)
        or_op = BinOp(and_op, "OR", third_compare)

        self.assertEqual(ast, or_op)

    def test_arithmetic_and_compare_exp(self):
        ast = get_ast_for_str("2 + 3 <= 6 - 1 * 1", "exp")
        first_arith_exp = BinOp(IntegerLiteral(2), "+", IntegerLiteral(3))
        second_arith_exp = BinOp(IntegerLiteral(6), "-", BinOp(IntegerLiteral(1), "*", IntegerLiteral(1)))

        self.assertEqual(BinOp(first_arith_exp, "<=", second_arith_exp), ast)

    def test_is_null(self):
        ast = get_ast_for_str("fld IS NULL", "exp")
        expected = BinOp(Field("fld"), "IS", NullLiteral())

        self.assertEqual(expected, ast)

    def test_is_not_null(self):
        ast = get_ast_for_str("fld IS NOT NULL", "exp")
        expected = BinOp(Field("fld"), "IS NOT", NullLiteral())

        self.assertEqual(expected, ast)

    def test_not_equals(self):
        expected_str = "1 <> 2"
        ast = get_ast_for_str("1 != 2", "exp")

        self.assertEqual(expected_str, str(ast))

        expected_ast = BinOp(IntegerLiteral(1), "<>", IntegerLiteral(2))
        self.assertEqual(expected_ast, ast)

    def test_between(self):
        expected_str = "tbl.field BETWEEN 1 AND 2"
        ast = get_ast_for_str(expected_str, "exp")

        self.assertEqual(expected_str, str(ast))

        expected_ast = Between(Field("field", "tbl"), IntegerLiteral(1), IntegerLiteral(2))
        self.assertEqual(expected_ast, ast)

    def test_not_between(self):
        expected_str = "tbl.field NOT BETWEEN 1 AND 2"
        ast = get_ast_for_str(expected_str, "exp")

        self.assertEqual(expected_str, str(ast))

        expected_ast = Between(Field("field", "tbl"), IntegerLiteral(1), IntegerLiteral(2), True)
        self.assertEqual(expected_ast, ast)

    def test_like(self):
        expected_str = "tbl.field LIKE 'abc%'"
        ast = get_ast_for_str(expected_str, "exp")

        self.assertEqual(expected_str, str(ast))

        expected_ast = BinOp(Field("field", "tbl"), 'LIKE', StringLiteral("abc%"))
        self.assertEqual(expected_ast, ast)

    def test_not_like(self):
        expected_str = "tbl.field NOT LIKE 'abc%'"
        ast = get_ast_for_str(expected_str, "exp")

        self.assertEqual(expected_str, str(ast))

        expected_ast = BinOp(Field("field", "tbl"), 'NOT LIKE', StringLiteral("abc%"))
        self.assertEqual(expected_ast, ast)
