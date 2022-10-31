class InClause:
    def __init__(self, exp, exp_list, not_in=False):
        self._exp = exp
        self._exp_list = exp_list
        self._not_in = not_in

    @property
    def exp(self):
        return self._exp

    @property
    def exp_list(self):
        return self._exp_list

    @property
    def not_in(self):
        return self._not_in

    def __eq__(self, other):
        if isinstance(other, self.__class__) and len(self.exp_list) == len(other.exp_list) and self.exp == other.exp \
                and self.not_in == other.not_in:
            for i, exp in enumerate(self.exp_list):
                if other.exp_list[i] != self.exp_list[i]:
                    return False
            return True
        return False

    def __str__(self):
        result = str(self.exp) + " "
        if self.not_in:
            result += "NOT "
        result += "IN (" + ", ".join([str(exp) for exp in self.exp_list]) + ")"
        return result

    def accept(self, visitor):
        visitor.visit_in_clause(self)
