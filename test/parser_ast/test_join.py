from transform_generator.parser.ast.from_clause import FromClause
from transform_generator.parser.ast.join import Join
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.paren_exp import ParenExp
from transform_generator.parser.transform_exp import get_ast_for_str
import unittest


class JoinTest(unittest.TestCase):
    def test_basic_join(self):
        ast = get_ast_for_str("SELECT tbl2.field FROM tbl1 JOIN tbl2 ON (tbl1.field2 = tbl2.field3)")
        condition = ParenExp(BinOp(Field("field2", "tbl1"), "=", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"), FromClause("tbl1", [Join(None, "tbl2", condition, None)]))

        self.assertEqual(ast, expected)

    def test_basic_two_join(self):
        qry_str = "tbl2.field FROM tbl1 JOIN tbl2 ON (tbl1.field2 = tbl2.field3) "
        qry_str += "JOIN tbl3 ON (tbl3.field = tbl2.field)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(str(ast), qry_str)

        condition1 = ParenExp(BinOp(Field("field2", "tbl1"), "=", Field("field3", "tbl2")))
        condition2 = ParenExp(BinOp(Field("field", "tbl3"), "=", Field("field", "tbl2")))
        from_clause = FromClause("tbl1", [Join(None, "tbl2", condition1, None), Join(None, "tbl3", condition2, None)])
        expected = TransformExp(Field("field", "tbl2"), from_clause)

        self.assertEqual(ast, expected)

    def test_basic_two_join_with_underscore(self):
        qry_str = " SELECT tbl2.field FROM tbl1 JOIN tbl2 ON (tbl1._field2 = tbl2.field3) "
        ast = get_ast_for_str(qry_str)
        left_join_ast = ast.from_clause.joins[0].condition.exp.left.table_name + "." + ast.from_clause.joins[0].condition.exp.left.field_name
        self.assertEqual(left_join_ast, "tbl1._field2")

    def test_basic_join_with_alias(self):
        qry_str = "t2.field FROM tbl1 AS t1 JOIN tbl2 AS t2 ON (t1.field2 = t2.field3) "
        qry_str += "JOIN tbl3 AS t3 ON (t3.field = t2.field)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(str(ast), qry_str)

        condition1 = ParenExp(BinOp(Field("field2", "t1"), "=", Field("field3", "t2")))
        condition2 = ParenExp(BinOp(Field("field", "t3"), "=", Field("field", "t2")))
        from_clause = FromClause("tbl1", [Join(None, "tbl2", condition1, None, 't2'), Join(None, "tbl3", condition2,
                                                                                           None, 't3')], None, 't1')
        expected = TransformExp(Field("field", "t2"), from_clause)

        self.assertEqual(ast, expected)

    def test_basic_join_with_db_name(self):
        ast = get_ast_for_str("SELECT tbl2.field FROM tbl1 JOIN db.tbl2 ON (tbl1.field2 = tbl2.field3)")
        condition = ParenExp(BinOp(Field("field2", "tbl1"), "=", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"), FromClause("tbl1", [Join(None, "tbl2", condition, "db")]))

        self.assertEqual(ast, expected, "AST for Join with DB name did not match expected.")

    def test_basic_join_with_alias_and_db_name(self):
        qry = "t2.field FROM db.tbl1 AS t1 JOIN db.tbl2 AS t2 ON (t1.field2 = t2.field3)"
        ast = get_ast_for_str(qry)
        condition = ParenExp(BinOp(Field("field2", "t1"), "=", Field("field3", "t2")))
        expected = TransformExp(Field("field", "t2"), FromClause("tbl1", [Join(None, "tbl2", condition, "db", 't2'
                                                                               )], 'db', 't1'))
        self.assertEqual(str(ast), qry)
        self.assertEqual(ast, expected, "AST for Join with DB name did not match expected.")

    def test_basic_join_with_db_name_and_table_underscore(self):
        ast = get_ast_for_str("SELECT _tbl2.field FROM _db._tbl1 JOIN _db._tbl2 ON (_tbl1.field2 = _tbl2.field3)")
        self.assertEqual(ast.from_clause.joins[0].database_name, "_db")
        self.assertEqual(ast.from_clause.joins[0].table_name, "_tbl2")
        self.assertEqual(ast.from_clause.database_name, "_db")
        self.assertEqual(ast.from_clause.table_name, "_tbl1")

    def test_left_outer_join(self):
        qry = "tbl2.field FROM tbl1 LEFT OUTER JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)"
        ast = get_ast_for_str(qry)
        self.assertEqual(str(ast), qry)

        condition = ParenExp(BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("LEFT OUTER", "tbl2", condition, None)]))

        self.assertEqual(expected, ast)

    def test_full_outer_join(self):
        sql_str = "tbl2.field FROM tbl1 FULL OUTER JOIN tbl2 ON tbl1.field2 <> tbl2.field3"
        ast = get_ast_for_str(sql_str)
        condition = BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2"))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("FULL OUTER", "tbl2", condition, None)]))

        self.assertEqual(str(ast), sql_str)
        self.assertEqual(expected, ast)

    def test_right_outer_join(self):
        sql_str = "tbl2.field FROM tbl1 RIGHT OUTER JOIN tbl2 ON tbl1.field2 <> tbl2.field3"
        ast = get_ast_for_str(sql_str)
        condition = BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2"))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("RIGHT OUTER", "tbl2", condition, None)]))

        self.assertEqual(str(ast), sql_str)
        self.assertEqual(expected, ast)

    def test_inner_join(self):
        sql_str = "tbl2.field FROM tbl1 INNER JOIN tbl2 ON tbl1.field2 <> tbl2.field3"
        ast = get_ast_for_str(sql_str)
        condition = BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2"))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("INNER", "tbl2", condition, None)]))

        self.assertEqual(str(ast), sql_str)
        self.assertEqual(expected, ast)

    def test_inner_join(self):
        sql_str = "tbl2.field FROM tbl1 CROSS JOIN tbl2"
        ast = get_ast_for_str(sql_str)
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("CROSS", "tbl2", None, None)]))

        self.assertEqual(str(ast), sql_str)
        self.assertEqual(expected, ast)

    def test_left_join(self):
        ast = get_ast_for_str("tbl2.field FROM tbl1 LEFT JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")
        self.assertEqual(str(ast), "tbl2.field FROM tbl1 LEFT OUTER JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")

        condition = ParenExp(BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("LEFT OUTER", "tbl2", condition, None)]))

        self.assertEqual(expected, ast)

    def test_right_join(self):
        ast = get_ast_for_str("tbl2.field FROM tbl1 RIGHT JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")
        self.assertEqual(str(ast), "tbl2.field FROM tbl1 RIGHT OUTER JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")

        condition = ParenExp(BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("RIGHT OUTER", "tbl2", condition, None)]))

        self.assertEqual(expected, ast)

    def test_full_join(self):
        ast = get_ast_for_str("tbl2.field FROM tbl1 FULL JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")
        self.assertEqual(str(ast), "tbl2.field FROM tbl1 FULL OUTER JOIN tbl2 ON (tbl1.field2 <> tbl2.field3)")

        condition = ParenExp(BinOp(Field("field2", "tbl1"), "<>", Field("field3", "tbl2")))
        expected = TransformExp(Field("field", "tbl2"),
                                FromClause("tbl1", [Join("FULL OUTER", "tbl2", condition, None)]))

        self.assertEqual(expected, ast)

    def test_get_dependencies(self):
        ast = get_ast_for_str("tbl2.field FROM tbl1 LEFT OUTER JOIN tbl2 ON tbl1.field2 <> tbl2.field3")
        dependencies = ast.from_clause.get_table_dependencies()

        self.assertEqual(dependencies, {"tbl1", "tbl2"})

    def test_single_dependency(self):
        ast = get_ast_for_str("tbl1.field FROM tbl1")
        dependencies = ast.from_clause.get_table_dependencies()
        self.assertEqual(dependencies, {"tbl1"})
