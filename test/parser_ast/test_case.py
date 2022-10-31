from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.case import Case
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.transform_exp import get_ast_for_str
import unittest


class CaseTest(unittest.TestCase):
    def test_simple_case(self):
        case_str = "CASE field WHEN 'A' THEN 1 ELSE 0 END"
        ast = get_ast_for_str(case_str)

        self.assertEqual(str(ast), case_str)
        when_clauses = [(StringLiteral("A"), IntegerLiteral(1))]
        expected = TransformExp(Case(Field("field"), when_clauses, IntegerLiteral(0)))

        self.assertEqual(ast, expected)

    def test_multi_when_case(self):
        case_str = "CASE field WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 0 END"
        ast = get_ast_for_str(case_str)

        self.assertEqual(str(ast), case_str)
        when_clauses = [(StringLiteral("A"), IntegerLiteral(1)), (StringLiteral("B"), IntegerLiteral(2))]
        expected = TransformExp(Case(Field("field"), when_clauses, IntegerLiteral(0)))

        self.assertEqual(ast, expected)

    def test_func_exp(self):
        case_str = "CASE substr('ABC',1,1) WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 0 END"
        ast = get_ast_for_str(case_str)

        self.assertEqual(str(ast), case_str)
        when_clauses = [(StringLiteral("A"), IntegerLiteral(1)), (StringLiteral("B"), IntegerLiteral(2))]
        func = FunctionCall("substr", [IntegerLiteral(1), IntegerLiteral(1)])
        expected = TransformExp(Case(func, when_clauses, IntegerLiteral(0)))

        self.assertEqual(ast, expected)

    def test_alternate_case(self):
        case_str = "CASE WHEN field = 'A' THEN 1 ELSE 0 END"
        ast = get_ast_for_str(case_str)

        self.assertEqual(str(ast), case_str)
        when_clauses = [(BinOp(Field("field"), "=", StringLiteral('A')), IntegerLiteral(1))]
        expected = TransformExp(Case(None, when_clauses, IntegerLiteral(0)))

        self.assertEqual(ast, expected)

