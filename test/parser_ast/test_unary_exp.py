import unittest

from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.unary_exp import UnaryExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.transform_exp import get_ast_for_str


class TestUnaryCase(unittest.TestCase):
    def test_unary_exp(self):
        ast = get_ast_for_str('-1')
        expected = TransformExp(UnaryExp('-', IntegerLiteral(1)), None)
        self.assertEqual(ast, expected, "Unary expression (-) AST does not match expected.")

    def test_unary_not_exp(self):
        ast = get_ast_for_str('NOT field')
        expected = TransformExp(UnaryExp('NOT', Field('field')))
        self.assertEqual(ast, expected, "Unary expression (NOT) AST does not match expected.")


if __name__ == '__main__':
    unittest.main()
