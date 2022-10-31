class BinOp:
    def __init__(self, left, op: str, right):
        self._left = left
        self._op = op
        self._right = right

    @property
    def left(self):
        return self._left

    @property
    def op(self):
        return self._op

    @property
    def right(self):
        return self._right

    def __str__(self):
        return str(self.left) + " " + self.op + " " + str(self.right)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.left == other.left and self.op == other.op and self.right == other.right

    def accept(self, visitor):
        visitor.visit_bin_op(self)
