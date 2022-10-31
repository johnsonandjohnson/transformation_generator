from transform_generator.parser.ast.decimal_literal import DecimalLiteral
from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.integer_literal import IntegerLiteral
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.null_literal import NullLiteral
from transform_generator.parser.syntax_error_list_exception import SyntaxErrorListException

import unittest


class FieldTestCase(unittest.TestCase):
    def test_simple_field(self):
        ast = get_ast_for_str("table.field")
        expected = TransformExp(Field('field', 'table'))
        self.assertEqual(ast, expected, "Simple Field AST does not match expected.")

    def test_simple_field_with_underscore(self):
        ast = get_ast_for_str("table._field")
        self.assertEqual(ast.result_column.table_name + "." + ast.result_column.field_name, "table._field")

    @staticmethod
    def test_simple_field_with_from():
        ast = get_ast_for_str("field FROM table")
        expected = TransformExp(Field('field'), FromClause('table', None))
        assert ast == expected, "Simple Field with FROM clause AST does not match expected."

    @staticmethod
    def test_integer_literal():
        ast = get_ast_for_str("1")
        expected = TransformExp(IntegerLiteral(1), None)
        assert ast == expected, "Number literal AST does not match expected. "

    @staticmethod
    def test_decimal_literal():
        ast = get_ast_for_str("1.1")
        expected = TransformExp(DecimalLiteral(1.1), None)
        assert ast == expected, "Decimal literal AST does not match expected. "


    @staticmethod
    def test_string_literal():
        ast = get_ast_for_str("'abc'")
        expected = TransformExp(StringLiteral("abc"), None)
        assert ast == expected, "String literal AST does not match expected. "

    @staticmethod
    def test_select_literal_no_from():
        ast = get_ast_for_str("SELECT 123")
        expected = TransformExp(IntegerLiteral(123), None)

        assert ast == expected

    @staticmethod
    def test_select_literal_with_from():
        ast = get_ast_for_str("SELECT 'xyz' FROM tbl2")
        expected = TransformExp(StringLiteral("xyz"), FromClause("tbl2", []))
        assert ast == expected

    def test_database_name_in_from(self):
        ast = get_ast_for_str("field FROM db.table")
        expected = TransformExp(Field('field'), FromClause('table', None, "db"))
        self.assertEqual(ast, expected, "Simple Field with database in FROM clause AST does not match expected.")

    def test_database_name_in_from_with_field_underscore(self):
        ast = get_ast_for_str("_field FROM db.table")
        self.assertEqual(ast.result_column.field_name, '_field')

    def test_syntax_error(self):
        self.assertRaises(SyntaxErrorListException, get_ast_for_str, "field FORM db.table")

    def test_null_literal(self):
        ast = get_ast_for_str("NULL")
        expected = TransformExp(NullLiteral(), None, None)
        self.assertEqual(ast, expected)

    def test_distinct(self):
        qry_str = "DISTINCT field FROM table"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))
        expected = TransformExp(Field("field"), FromClause("table"), distinct=True)
        self.assertEqual(ast, expected)

    def test_key_word_field(self):
        ast = get_ast_for_str('table.`location`')
        expected = TransformExp(Field('location', 'table'))
        self.assertEqual(ast, expected,
                         'Key Word Field AST does not match expected.')


if __name__ == '__main__':
    unittest.main()
