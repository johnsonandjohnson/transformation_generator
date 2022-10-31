
class ParenExp:
    """
    This class represents an expression which is surrounded by parenthesis.
    It is, itself, an expression. Therefore, ParenExp._exp may contain a ParenExp recursively.
    
    """
    def __init__(self, exp):
        self._exp = exp
        
    @property
    def exp(self):
        return self._exp
    
    def __str__(self):
        return "(" + str(self._exp) + ")"
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._exp == other._exp
        return False

    def accept(self, visitor):
        visitor.visit_paren_exp(self)
