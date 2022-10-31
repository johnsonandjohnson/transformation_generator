class Case:
    def __init__(self, exp, when_clauses, else_clause=None):
        self._exp = exp
        self._when_clauses = when_clauses
        self._else_clause = else_clause

    @property
    def exp(self):
        return self._exp

    @property
    def when_clauses(self):
        return self._when_clauses

    @property
    def else_clause(self):
        return self._else_clause

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            else_clause_match = True if self._else_clause is None and other._else_clause is None or \
                self._else_clause == other._else_clause else False

            when_clauses_match = False

            if len(self._when_clauses) == len(other._when_clauses):
                when_clauses_match = True
                for i, clause in enumerate(self._when_clauses):
                    other_clause = other._when_clauses[i]
                    if clause[0] != other_clause[0] or clause[1] != other_clause[1]:
                        when_clauses_match = False

            return when_clauses_match and else_clause_match
        return False

    def __str__(self):
        result = "CASE"
        if self._exp is not None:
            result += " " + str(self._exp)
        for condition, result_exp in self._when_clauses:
            result += " WHEN " + str(condition) + " THEN " + str(result_exp)

        if self._else_clause is not None:
            result += " ELSE " + str(self._else_clause)
        return result + " END"

    def accept(self, visitor):
        visitor.visit_case(self)
