class NullLiteral:
    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __str__(self):
        return "NULL"

    def accept(self, visitor):
        visitor.visit_null_literal(self)
