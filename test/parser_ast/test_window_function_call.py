from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.function_call import FunctionCall
from transform_generator.parser.ast.string_literal import StringLiteral
from transform_generator.parser.ast.case import Case
from transform_generator.parser.ast.bin_op import BinOp
from transform_generator.parser.ast.window_function_call import WindowFunctionCall

import unittest


class TestWindowFunctionCall(unittest.TestCase):
    def test_partition_by(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY a, tbl.b)"
        ast = get_ast_for_str("FIRST(tbl.field) OVER (PARTITION BY a, tbl.b)")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Field("a"), Field("b", "tbl")]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by))

        self.assertEqual(ast, expected)

    def test_partition_by_with_exp(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY TRIM(' test case '))"
        ast = get_ast_for_str("FIRST(tbl.field) OVER (PARTITION BY TRIM(' test case '))")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [FunctionCall("TRIM", [StringLiteral(' test case ')])]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by))

        self.assertEqual(ast, expected)

    def test_partition_by_with_concat_exp(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY CONCAT('test','case'))"
        ast = get_ast_for_str("FIRST(tbl.field) OVER (PARTITION BY CONCAT('test','case'))")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [FunctionCall("CONCAT", [StringLiteral('test'), StringLiteral('case')])]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by))

        self.assertEqual(ast, expected)

    def test_partition_by_and_order_by(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY a, tbl.b ORDER BY b, c)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Field("a"), Field("b", "tbl")]
        order_by = [(Field("b"), None), (Field("c"), None)]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_order_by(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY b, c)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Field("b"), None), (Field("c"), None)]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_order_by_desc(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY b, c DESC)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Field("b"), None), (Field("c"), 'DESC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_order_by_asc(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY b, c ASC)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Field("b"), None), (Field("c"), 'ASC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_order_by_asc_desc(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY b ASC, c DESC)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Field("b"), 'ASC'), (Field("c"), 'DESC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_order_by_desc_asc(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY b DESC, c ASC)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Field("b"), 'DESC'), (Field("c"), 'ASC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_partition_by_and_order_by_asc_desc(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY a, tbl.b ORDER BY b ASC, c DESC)"
        ast = get_ast_for_str(qry_str)
        self.assertEqual(qry_str, str(ast))

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Field("a"), Field("b", "tbl")]
        order_by = [(Field("b"), 'ASC'), (Field("c"), 'DESC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))

        self.assertEqual(ast, expected)

    def test_partition_by_with_count(self):
        qry_str = "COUNT(*) OVER (PARTITION BY a, tbl.b)"
        ast = get_ast_for_str("COUNT(*) OVER (PARTITION BY a, tbl.b)")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("COUNT", [], False, True)
        partition_by = [Field("a"), Field("b", "tbl")]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by))

        self.assertEqual(ast, expected)

    def test_partition_by_with_case(self):
        qry_str = "FIRST(tbl.field) OVER (PARTITION BY CASE WHEN field = 'A' THEN a ELSE tbl.b END)"
        ast = get_ast_for_str("FIRST(tbl.field) OVER (PARTITION BY CASE WHEN field = 'A' THEN a ELSE tbl.b END)")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = [Case(None, [(BinOp(Field("field"), "=", StringLiteral("A")), Field("a"))], Field("b", "tbl"))]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by))
        self.assertEqual(ast, expected)

    def test_order_by_with_case(self):
        qry_str = "FIRST(tbl.field) OVER (ORDER BY CASE WHEN field = 'A' THEN a ELSE b END ASC)"
        ast = get_ast_for_str("FIRST(tbl.field) OVER (ORDER BY CASE WHEN field = 'A' THEN a ELSE b END ASC)")
        self.assertEqual(str(ast), qry_str)

        function_call = FunctionCall("FIRST", [Field("field", "tbl")])
        partition_by = []
        order_by = [(Case(None, [(BinOp(Field("field"), "=", StringLiteral("A")), Field("a"))], Field("b")), 'ASC')]
        expected = TransformExp(WindowFunctionCall(function_call, partition_by, order_by))
        self.assertEqual(ast, expected)
