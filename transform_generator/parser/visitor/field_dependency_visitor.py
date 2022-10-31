from transform_generator.parser.ast.field import Field
from transform_generator.parser.ast.transform_exp import TransformExp

from .ast_visitor import AstVisitor


class FieldDependencyVisitor(AstVisitor):
    def __init__(self):
        self._alias_table_name_map = None
        self._field_dependencies = set()
        self._table_dependencies = set()
        self._missing_or_invalid_aliases = set()

    def clear(self):
        self._alias_table_name_map = None
        self._field_dependencies.clear()
        self._table_dependencies.clear()
        self._missing_or_invalid_aliases.clear()

    @property
    def field_dependencies(self):
        return self._field_dependencies

    @property
    def missing_or_invalid_aliases(self):
        return self._missing_or_invalid_aliases

    def visit_field(self, field: Field):
        field_name = field.field_name

        table_name = field.table_name
        if table_name is None:
            if len(self._table_dependencies) == 1:
                table_name = list(self._table_dependencies)[0]
            else:
                self._missing_or_invalid_aliases.add(str(field))
                return

        if self._alias_table_name_map is not None:
            if table_name not in self._alias_table_name_map.keys():
                self._missing_or_invalid_aliases.add(str(field))
                return

            table_name = self._alias_table_name_map[table_name]
        field_name = table_name + "." + field_name

        self._field_dependencies.add(field_name)

    def visit_transform_exp(self, transform_exp: TransformExp):
        if transform_exp.from_clause is not None:
            self._alias_table_name_map = transform_exp.from_clause.alias_table_name_map
            self._table_dependencies = transform_exp.from_clause.get_table_dependencies()

        # We only examine the result_column for now
        transform_exp.result_column.accept(self)
