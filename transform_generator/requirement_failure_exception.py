class RequirementFailureException(Exception):
    def __init__(self, error_msg):
        super().__init__()
        self._error_msg = error_msg

    @property
    def error_msg(self):
        return self._error_msg
