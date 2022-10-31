class AliasedResultCol:
    def __init__(self, exp, alias):
        self._exp = exp
        self._alias = alias

    @property
    def exp(self):
        return self._exp

    @property
    def alias(self):
        return self._alias

    def __str__(self):
        return str(self.exp) + " AS " + self.alias

    def __eq__(self, other):
        return self.exp == other.exp and self.alias == other.alias

    def accept(self, visitor):
        visitor.visit_aliased_result_col(self)
