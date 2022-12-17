from os.path import join, split, exists
from typing import List
from dataclasses import dataclass, field

from transform_generator.lib import config
from transform_generator.lib.config_entry import ConfigEntry
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.lib.project_config_entry import ProjectConfigEntry
from transform_generator.lib.mapping import load_mappings
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.reader.config_reader import read_config_csv
from transform_generator.reader.table_definition_reader import read_table_definition


@dataclass(order=True)
class DataMappingGroup:
    name: str
    data_mappings: List[DataMapping]
    tables_by_db_table_name: dict[str, TableDefinition]
    mapping_group_config: ProjectConfigEntry = None
    programs: List[ConfigEntry] = field(default_factory=list)
