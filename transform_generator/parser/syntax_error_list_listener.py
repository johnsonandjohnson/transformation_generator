from antlr4.error.ErrorListener import ErrorListener


class SyntaxErrorListListener(ErrorListener):
    def __init__(self):
        self._error_list = []

    def clear(self):
        self._error_list.clear()

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        self._error_list.append("line " + str(line) + ":" + str(column) + " " + msg)

    @property
    def error_msg_list(self):
        return self._error_list
