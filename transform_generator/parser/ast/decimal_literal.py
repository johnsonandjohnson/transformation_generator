class DecimalLiteral:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __str__(self):
        return str(self._value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return False

    def accept(self, visitor):
        visitor.visit_decimal_literal(self)
