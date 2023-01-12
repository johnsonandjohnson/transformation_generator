
from typing import List
from dataclasses import dataclass, field

from transform_generator.lib.data_mapping import BaseMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry


@dataclass(order=True)
class DataMappingGroup:
    name: str
    data_mappings: List[BaseMapping]
    mapping_group_config: ProjectConfigEntry = None
