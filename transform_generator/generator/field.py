from typing import NamedTuple


class Field(NamedTuple):
    name: str
    data_type: str
    nullable: bool
    column_description: str
    scale: int = None
    precision: int = None
