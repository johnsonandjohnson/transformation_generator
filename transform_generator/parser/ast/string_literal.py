class StringLiteral:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __str__(self):
        return "'" + self._value + "'"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return False

    def accept(self, visitor):
        visitor.visit_string_literal(self)
