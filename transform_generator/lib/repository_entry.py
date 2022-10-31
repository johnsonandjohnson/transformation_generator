class RepositoryEntry:
    def __init__(self, name, version=None, ddl_only=False, is_orchestration=False, project_config_path=None):
        self._name = name
        self._version = version
        self._ddl_only = ddl_only
        self._is_orchestration = is_orchestration
        self._project_config_path = project_config_path

    @property
    def name(self):
        return self._name

    @property
    def version(self):
        return self._version

    @property
    def ddl_only(self):
        return self._ddl_only

    @property
    def is_orchestration(self):
        return self._is_orchestration

    @property
    def project_config_path(self):
        return self._project_config_path

    @is_orchestration.setter
    def is_orchestration(self, is_orchestration):
        self._is_orchestration = is_orchestration

    @ddl_only.setter
    def ddl_only(self, ddl_only):
        self._ddl_only = ddl_only

    @project_config_path.setter
    def project_config_path(self, project_config_path):
        self._project_config_path = project_config_path
