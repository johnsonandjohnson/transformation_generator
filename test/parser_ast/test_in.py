from transform_generator.parser.ast.in_clause import InClause
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.transform_exp import get_ast_for_str

import unittest


class InTestCase(unittest.TestCase):
    def test_simple_in(self):
        expected_str = "tbl.field1 FROM tbl WHERE tbl.field1 IN ('A', 'B', 'C')"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field1", "tbl")
        in_clause = InClause(field, [StringLiteral("A"), StringLiteral("B"), StringLiteral("C")])
        expected_ast = TransformExp(field, FromClause("tbl"), in_clause)

        self.assertEqual(expected_ast, ast)

    def test_not_in(self):
        expected_str = "tbl.field1 FROM tbl WHERE tbl.field1 NOT IN ('A', 'B', 'C')"
        ast = get_ast_for_str(expected_str)

        self.assertEqual(expected_str, str(ast))

        field = Field("field1", "tbl")
        in_clause = InClause(field, [StringLiteral("A"), StringLiteral("B"), StringLiteral("C")], True)
        expected_ast = TransformExp(field, FromClause("tbl"), in_clause)

        self.assertEqual(expected_ast, ast)


