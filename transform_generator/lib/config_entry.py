from dataclasses import dataclass, field
from typing import List


@dataclass(order=True)
class ConfigEntry:
    key: str
    target_table: str
    target_type: str
    target_language: str
    input_files: List[str]
    dependencies: str
    tuning_parameter: List[str]
    load_type: str
    load_synapse: str
    sequence: int

    def __post_init__(self):
        self.sequence = int(self.sequence) if self.sequence else None
