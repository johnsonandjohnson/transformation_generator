from pydantic import BaseModel, conlist, constr, StrictStr
from transform_generator.api.dto.data_mapping_response_dto import DataMappingResponseDTO

class QueryConfigDTO(BaseModel):
    """A DTO used as a response object, which maps a table name to a list of transformation queries."""
    target_table: StrictStr
    data_mapping_responses: conlist(DataMappingResponseDTO, min_items=0)

