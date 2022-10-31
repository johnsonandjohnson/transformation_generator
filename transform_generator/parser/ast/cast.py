class Cast:
    def __init__(self, parameter, data_type: str):
        self._parameter = parameter
        self._data_type = data_type

    @property
    def parameter(self):
        return self._parameter

    @property
    def data_type(self):
        return self._data_type

    def __str__(self):
        return "CAST(" + str(self.parameter) + " AS " + str(self.data_type) + ")"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.parameter == self.parameter and other.data_type == self.data_type
        return False

    def accept(self, visitor):
        visitor.visit_cast(self)
