from dataclasses import dataclass

from transform_generator.PathParameters import PathParameters


@dataclass
class DdlPathParameters(PathParameters):
    output_dir_databricks: str
    output_dir_datafactory: str
    target_notebook_folder: str
    external_module_config_paths: str
