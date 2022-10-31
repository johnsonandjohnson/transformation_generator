class WindowFunctionCall:
    def __init__(self, function_call, partition_by=[], order_by=[]):
        self._function_call = function_call
        self._partition_by = partition_by
        self._order_by = order_by

    @property
    def function_call(self):
        return self._function_call

    @property
    def partition_by(self):
        return self._partition_by
    
    @property
    def order_by(self):
        return self._order_by

    def __str__(self):
        partition_by = ", ".join([str(field) for field in self._partition_by])
        order_by = ", ".join([str(exp) + ((" " + direction) if direction is not None else '')
                              for (exp, direction) in self._order_by])
        result = str(self._function_call) + " OVER ("
        if len(self._partition_by) > 0:
            result += "PARTITION BY " + partition_by
            if len(self._order_by) > 0:
                result += " "
        if len(self._order_by) > 0:
            result += "ORDER BY " + order_by
        return result + ")"        

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.function_call != other.function_call:
                return False
            if self._partition_by != other.partition_by:
                return False
            if self.order_by != other.order_by:
                return False
            return True
        return False

    def accept(self, visitor):
        visitor.visit_window_function_call(self)
