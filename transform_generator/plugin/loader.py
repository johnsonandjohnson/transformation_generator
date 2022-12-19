from transform_generator.plugin.project_loader import ProjectLoader
from transform_generator.plugin.dc_project_loader import DcProjectLoader


class PluginLoader:
    def project_group_loader(self) -> ProjectLoader:
        return DcProjectLoader()


_plugin_loader = PluginLoader()


def get_plugin_loader() -> PluginLoader:
    return _plugin_loader



