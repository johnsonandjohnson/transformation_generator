from dataclasses import dataclass
from typing import List

from transform_generator.data_mapping_group import DataMappingGroup


@dataclass
class Project:
    name: str
    data_mapping_groups: List[DataMappingGroup]
