from pydantic import BaseModel, conlist, constr, StrictStr, StrictInt
from typing import Optional
from transform_generator.api.dto.config_entry_dto import ConfigEntryDTO
from transform_generator.lib.project_config_entry import ProjectConfigEntry


# A DTO which is used to map API requests/responses to the ProjectConfig business object.
class ConfigEntryGroupDTO(BaseModel):
    key: constr(min_length=1, strict=True)
    group_name: constr(min_length=1, strict=True)
    schedule_frequency: Optional[StrictStr]
    schedule_days: Optional[StrictStr]
    parallel_db_name: Optional[StrictStr]
    base_path: Optional[StrictStr]
    notebooks_to_execute: Optional[StrictStr]
    cluster_name: Optional[StrictStr]
    config_entries: conlist(ConfigEntryDTO, min_items=1)
    ddl_wrapper_script: Optional[StrictStr]
    sequence: Optional[StrictInt]

    @staticmethod
    def from_business_object(src_object: ProjectConfigEntry, config_entry_dto: [ConfigEntryDTO]):
        """
        Acts as a factory method to generate a DTO from a business-logic-layer object.
        @param instance_key: A string to be used as a unique identifier for the DTO
        @param data_mapping: The business object to convert from.
        @return:    A new instance of the DTO based on the business object.
        """
        return ConfigEntryGroupDTO(
            key=src_object.config_filename,
            group_name=src_object.group_name,
            schedule_frequency=src_object.schedule_frequency,
            schedule_days=src_object.schedule_days,
            parallel_db_name=src_object.parallel_db_name,
            base_path=src_object.base_path,
            notebooks_to_execute=src_object.notebooks_to_execute,
            cluster_name=src_object.cluster_name,
            config_entries=config_entry_dto,
            ddl_wrapper_script=src_object.ddl_wrapper_script,
            sequence=src_object.sequence
            )

    def to_business_object(self) -> ProjectConfigEntry:
        """
        Converts the DTO to a business object to be used in business logic.
        @return:    A new instance of business object based on the DTO.
        """
        return ProjectConfigEntry(
            self.key,
            self.group_name,
            self.schedule_frequency,
            self.schedule_days,
            self.parallel_db_name,
            self.base_path,
            self.notebooks_to_execute,
            self.cluster_name,
            self.ddl_wrapper_script,
            self.sequence
        )
