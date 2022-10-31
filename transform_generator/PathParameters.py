from dataclasses import dataclass


@dataclass
class PathParameters:
    config_path: str
    schema_path: str
    mapping_path: str
    project_config_paths: str
