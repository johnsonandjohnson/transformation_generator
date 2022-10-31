from pydantic import BaseModel, conlist, constr
from transform_generator.api.dto.config_entry_group_dto import ConfigEntryGroupDTO


# A DTO which represents the entire transformation generator context for a request.
class ProjectConfigDTO(BaseModel):
    key: constr(min_length=1, strict=True)
    config_entry_groups: conlist(ConfigEntryGroupDTO, min_items=1)
