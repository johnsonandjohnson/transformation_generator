class Between:
    def __init__(self, test_exp, begin_exp, end_exp, not_modifier=False):
        self._test_exp = test_exp
        self._begin_exp = begin_exp
        self._end_exp = end_exp
        self._not_modifier = not_modifier
        
    @property
    def test_exp(self):
        return self._test_exp

    @property
    def begin_exp(self):
        return self._begin_exp

    @property
    def end_exp(self):
        return self._end_exp

    @property
    def not_modifier(self):
        return self._not_modifier
    
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._test_exp == other._test_exp and self._begin_exp == other._begin_exp and \
            self._end_exp == other._end_exp and self._not_modifier == other._not_modifier

    def __str__(self):
        result = str(self._test_exp) + " "
        if self._not_modifier:
            result += "NOT "
        return result + "BETWEEN " + str(self._begin_exp) + " AND " + str(self._end_exp)

    def accept(self, visitor):
        visitor.visit_between(self)
