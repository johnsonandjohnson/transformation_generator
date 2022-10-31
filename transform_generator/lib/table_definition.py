from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List

from transform_generator.generator.field import Field


@dataclass
class TableDefinition:
    database_name: str
    table_name: str
    table_business_name: str
    table_description: str
    fields: Dict[str, Field]
    partitioned_fields: OrderedDict[str]
    primary_keys: List[str]
    non_partitioned_fields: OrderedDict[str] = field(default_factory=OrderedDict)

    def __post_init__(self):
        self.non_partitioned_fields = OrderedDict((field_name, field) for field_name, field in self.fields.items()
                                                  if field_name not in self.partitioned_fields.keys())
