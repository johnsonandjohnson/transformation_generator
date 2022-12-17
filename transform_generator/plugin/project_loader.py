from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.project import Project


class ProjectLoader:
    """Interface for plug-ins to load a project group"""
    def load_project_group(self, project_group_config_path: str, project_base_path: str) -> list[Project]:
        pass

    def load_project(self, project_path: str) -> Project:
        pass

    def load_mapping(self, mapping_path: str) -> DataMapping:
        pass

    def load_table_definition(self, table_definition_path: str) -> TableDefinition:
        pass

