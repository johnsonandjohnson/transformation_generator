import unittest

from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.field import Field


class CaseTest(unittest.TestCase):
    def test_alias(self):
        ast = get_ast_for_str('field AS f')
        expected = TransformExp(AliasedResultCol(Field('field'), 'f'))
        self.assertEqual(ast, expected, "Simple alias AST does not match expected.")

    def test_no_alias(self):
        ast = get_ast_for_str('field')
        expected = TransformExp(Field('field'))
        self.assertEqual(ast, expected, "Simple no alias AST does not match expected.")
