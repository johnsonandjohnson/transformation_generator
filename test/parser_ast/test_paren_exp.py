import unittest

from transform_generator.parser.ast.paren_exp import ParenExp
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.transform_exp import get_ast_for_str


class CaseTest(unittest.TestCase):
    def test_paren_exp(self):
        ast = get_ast_for_str('(field)')
        expected = TransformExp(ParenExp(Field('field')))
        self.assertEqual(ast, expected, "Simple paren exp AST does not match expected.")

    def test_no_paren(self):
        ast = get_ast_for_str('field')
        expected = TransformExp(Field('field'))
        self.assertEqual(ast, expected, "Simple no paren exp AST does not match expected.")
