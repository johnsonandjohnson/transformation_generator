from pydantic import BaseModel, StrictStr

class DataMappingResponseDTO(BaseModel):
    data_mapping_key: StrictStr
    query: StrictStr
