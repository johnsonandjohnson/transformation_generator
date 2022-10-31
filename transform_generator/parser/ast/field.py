from transform_generator.parser.keywords import keywords


class Field:
    def __init__(self, field_name, table_name=None):
        self._table_name = table_name
        self._table_name = table_name
        self._field_name = field_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def field_name(self):
        return self._field_name

    def __str__(self):
        if self.field_name.lower() in keywords() or self.field_name[0] == '_':
            result = '`' + self.field_name + '`'
        else:
            result = self.field_name

        if self.table_name is not None:
            result = "`" + self.table_name + "`" if self.table_name[0] == "_" else self.table_name + "." + result
        return result

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.table_name == other.table_name and self.field_name == other.field_name
        return False

    def accept(self, visitor):
        visitor.visit_field(self)
