class GroupByClause:
    def __init__(self, exp_list, having_exp=None):
        self._exp_list = exp_list if exp_list is not None else []
        self._having_exp = having_exp

    @property
    def exp_list(self):
        return self._exp_list

    @property
    def having_exp(self):
        return self._having_exp

    def __str__(self):
        result = "GROUP BY " + ", ".join(str(exp) for exp in self.exp_list)
        if self.having_exp is not None:
            result += " HAVING " + str(self.having_exp)
        return result

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            group_by_field_set = set([str(field) for field in self.exp_list])
            other_group_by_field_set = set([str(field) for field in other.exp_list])
            group_by_field_match = group_by_field_set - other_group_by_field_set - group_by_field_set == set()
            return self.having_exp == other.having_exp and group_by_field_match
        return False

    def accept(self, visitor):
        visitor.visit_group_by_clause(self)
