class EnvironmentVariable:
    def __init__(self, env_var):
        self._env_var = env_var

    @property
    def env_var(self):
        return self._env_var

    def __str__(self):
        return '${' + self.env_var + '}'

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._env_var == other._env_var
        return False

    def accept(self, visitor):
        visitor.visit_environment_variable(self)
