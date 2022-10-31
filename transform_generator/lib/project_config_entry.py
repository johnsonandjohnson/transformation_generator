from dataclasses import dataclass


@dataclass
class ProjectConfigEntry:
    config_filename: str
    group_name: str
    schedule_frequency: str
    schedule_days: str
    parallel_db_name: str
    base_path: str
    notebooks_to_execute: str
    cluster_name: str
    ddl_wrapper_script: str
    sequence: int

    def __post_init__(self):
        self.sequence = int(self.sequence) if self.sequence else None