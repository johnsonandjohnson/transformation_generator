from pydantic import BaseModel, StrictStr, StrictBool, constr
from typing import Optional
from transform_generator.generator.field import Field


# A DTO which is used to represent a table column definition.
class FieldDTO(BaseModel):
    name: constr(min_length=1, strict=True)
    data_type: constr(min_length=1, strict=True)
    nullable: StrictBool
    primary_key: StrictBool
    column_description: Optional[StrictStr]

    @staticmethod
    def from_business_object(src_object: Field, primary_keys: [str] = []):
        """
        Acts as a factory method to generate a DTO from a business-logic-layer object.
        @param data_mapping: The business object to convert from.
        @return:    A new instance of the DTO based on the business object.
        """
        return FieldDTO(
            name=src_object.name,
            data_type=src_object.data_type,
            nullable=True if (src_object.nullable.strip() == 'NULL') else False,
            column_description=src_object.column_description,
            primary_key=src_object.name in primary_keys
        )

    def to_business_object(self) -> Field:
        """
        Converts the DTO to a business object to be used in business logic.
        @return:    A new instance of business object based on the DTO.
        """

        return Field(
            self.name,
            self.data_type,
            'NULL' if self.nullable else 'NOT NULL',
            self.column_description
        )
