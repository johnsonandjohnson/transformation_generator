
class SyntaxErrorListException(Exception):
    def __init__(self, error_msg_list):
        super().__init__()
        self._error_msg_list = error_msg_list

    @property
    def error_msg_list(self):
        return self._error_msg_list
