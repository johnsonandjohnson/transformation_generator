class FunctionCall:
    def __init__(self, name, parameters, distinct=False, asterisk=False):
        self._name = name
        self._parameters = parameters
        self._distinct = distinct
        self._asterisk = asterisk

    @property
    def name(self):
        return self._name

    @property
    def parameters(self):
        return self._parameters

    @property
    def distinct(self):
        return self._distinct

    @property
    def asterisk(self):
        return self._asterisk

    def __str__(self):
        result = self._name + "("
        if self._distinct:
            result += "DISTINCT "
        if self._asterisk:
            result += "*"
        elif self._parameters:
            param_str = ",".join([str(param) for param in self._parameters])
            result += param_str
        result += ")"
        return result

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.asterisk != other.asterisk:
                return False
            if self.name.lower() != other.name.lower() or self.parameters != other.parameters:
                return False
            for i, param in enumerate(other.parameters):
                if self.parameters[i] != param:
                    return False
            if self.distinct != other.distinct:
                return False
            # Name and all parameters are equal, so this is equal
            return True
        return False

    def accept(self, visitor):
        visitor.visit_function_call(self)
