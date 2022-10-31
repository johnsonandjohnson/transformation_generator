import unittest

from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.visitor.field_dependency_visitor import FieldDependencyVisitor


class FieldDependencyTest(unittest.TestCase):

    def test_field_dependencies(self):
        qry = """SELECT concatenate(table1.field1, substr(table2.field, 1, 1),
              CASE table3.field3 WHEN 'A' THEN table4.field4 WHEN 'B' THEN table5.field5 ELSE table6.field6 END)
              FROM database.table1
              JOIN table2
              JOIN db2.table3
              JOIN database.table4
              JOIN table5
              JOIN table6"""

        ast = get_ast_for_str(qry)
        visitor = FieldDependencyVisitor()
        ast.accept(visitor)

        expected_dependency_fields = {"database.table1.field1", "table2.field", "db2.table3.field3",
                                      "database.table4.field4", "table5.field5", "table6.field6"}

        self.assertEqual(expected_dependency_fields, visitor.field_dependencies)
