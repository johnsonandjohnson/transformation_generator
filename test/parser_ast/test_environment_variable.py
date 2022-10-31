from transform_generator.parser.ast.environment_variable import EnvironmentVariable
from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.bin_op import BinOp

import unittest


class EnvironmentVariableTest(unittest.TestCase):
    def test_env_var_where(self):
        expected_str = 'tbl.field FROM tbl WHERE field = ${processing_date}'
        ast = get_ast_for_str(expected_str)
        self.assertEqual(str(ast), expected_str)

        bin_op = BinOp(Field('field'), '=', EnvironmentVariable('processing_date'))
        expected = TransformExp(Field('field', 'tbl'), FromClause('tbl'), bin_op)
        self.assertEqual(ast, expected)

    def test_env_var(self):
        expected_str = '${processing_date}'
        ast = get_ast_for_str(expected_str)
        self.assertEqual(str(ast), expected_str)
        expected = TransformExp(EnvironmentVariable('processing_date'))
        self.assertEqual(ast, expected)
