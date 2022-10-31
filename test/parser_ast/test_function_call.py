from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.cast import Cast
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.transform_exp import get_ast_for_str

import unittest


class FunctionCallTestCase(unittest.TestCase):

    def test_func_no_params(self):
        ast = get_ast_for_str("CURRENT_TIMESTAMP()")
        expected = TransformExp(FunctionCall("CURRENT_TIMESTAMP", []))
        self.assertEqual(ast, expected)

    @staticmethod
    def testSubstr():
        """Test a simple function call for the "substr" method"""
        ast = get_ast_for_str("substr('abc', 1, 2) FROM table")
        expected = TransformExp(FunctionCall("substr", [StringLiteral('abc'), IntegerLiteral(1), IntegerLiteral(2)]),
                                FromClause('table', None))
        assert ast == expected, "AST does not match expected AST for input string."

    @staticmethod
    def test_nested_functions():
        ast = get_ast_for_str("concat(substr('xyz', 1, 2), concat('abc', substr('lmn', 1, 1)))")
        inner_cat = FunctionCall("concat", [StringLiteral("abc"), FunctionCall("substr", [StringLiteral('lmn'),
                                                                                          IntegerLiteral(1),
                                                                                          IntegerLiteral(1)])])
        expected = TransformExp(FunctionCall("concat", [FunctionCall("substr", [StringLiteral("xyz"), IntegerLiteral(1),
                                                                                IntegerLiteral(2)]), inner_cat]), None)
        assert ast == expected

    def test_cast(self):
        ast = get_ast_for_str("CAST(tbl.field AS STRING)")
        expected = TransformExp(Cast(Field("field", "tbl"), "STRING"))
        self.assertEqual(ast, expected)

    def test_cast_scale(self):
        ast = get_ast_for_str("CAST('abc' AS CHAR(1))")
        expected = TransformExp(Cast(StringLiteral('abc'), "CHAR(1)"))
        self.assertEqual(ast, expected)

    @staticmethod
    def test_nested_cast():
        ast = get_ast_for_str("CAST(CAST(substr('2020-01-01abc', 1, 10) AS DATE) AS STRING)")
        substr = FunctionCall("substr", [StringLiteral('2020-01-01abc'), IntegerLiteral(1), IntegerLiteral(10)])
        cast = Cast(Cast(substr, "DATE"), "STRING")
        expected = TransformExp(cast, None)
        assert ast == expected

    def test_if_with_is_null(self):
        ast = get_ast_for_str("if(fld IS NOT NULL, 'T', 'F')")
        expected = TransformExp(FunctionCall("if", [BinOp(Field("fld"), "IS NOT", NullLiteral()), StringLiteral("T"),
                                             StringLiteral("F")]))
        self.assertEqual(ast, expected)

    def test_count_with_distinct(self):
        ast = get_ast_for_str("COUNT(DISTINCT tbl.field)")
        expected = TransformExp(FunctionCall("COUNT", [Field("field", "tbl")], True))
        self.assertEqual(ast, expected)

    def test_count_asterisk(self):
        ast = get_ast_for_str("COUNT(*)")
        expected = TransformExp(FunctionCall("COUNT", [], False, True))
        self.assertEqual(ast, expected)

    def test_count_with_distinct_asterisk(self):
        ast = get_ast_for_str("COUNT(DISTINCT *)")
        expected = TransformExp(FunctionCall("COUNT", [], True, True))
        self.assertEqual(ast, expected)

    def test_asterisk_in_expression(self):
        ast = get_ast_for_str("COUNT(1 * 2)")
        expected = TransformExp(FunctionCall("COUNT", [BinOp(IntegerLiteral(1), '*', IntegerLiteral(2))], False, False))
        self.assertEqual(ast, expected)

    def test_asterisk_non_count_function(self):
        # asterisk should only be able to be used with count(), otherwise throw exception
        self.assertRaises(Exception, get_ast_for_str, "TRIM(*)")

    def test_char_ASCIICheck(self):
        ast = get_ast_for_str("concat('abc', CHAR(64), 'cde')")
        char = FunctionCall("CHAR", [IntegerLiteral(64)])
        expected = TransformExp(FunctionCall("concat", [StringLiteral('abc'), char, StringLiteral('cde')]))
        self.assertEqual(ast, expected)

if __name__ == '__main__':
    unittest.main()
