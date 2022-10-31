class UnaryExp:
    def __init__(self, op: str, exp):
        self._op = op
        self._exp = exp

    @property
    def exp(self):
        return self._exp

    @property
    def op(self):
        return self._op

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._exp == other._exp and self._op == other._op

    def __str__(self):
        return self.op + str(self.exp)

    def accept(self, visitor):
        visitor.visit_not_exp(self)
