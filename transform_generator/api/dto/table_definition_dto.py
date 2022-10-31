from pydantic import BaseModel, validator, conlist, constr, StrictStr
from typing import Optional, List
from transform_generator.api.dto.field_dto import FieldDTO
from transform_generator.lib.table_definition import TableDefinition
from transform_generator.api.dto.data_mapping_dto import DataMappingDTO


# A DTO which is used to map API requests/responses to the Table Definition business object.
class TableDefinitionDTO(BaseModel):
    database_name: constr(min_length=1, strict=True)
    fields: List[FieldDTO]
    partitioned_fields: Optional[List[StrictStr]]
    table_name: constr(min_length=1, strict=True)
    table_business_name: Optional[StrictStr]
    table_description: Optional[StrictStr]
    data_mappings: Optional[List[DataMappingDTO]]

    @validator('partitioned_fields')
    def validate_partitioned_fields(cls, partitioned_fields, values):
        """
        Validates that any column name also exists in a table definition.
        @param partitioned_fields:  The value of the field being validated.
        @param values:              All current object values
        @return:                 Returns the field value being validated, or throws an exception on validation failure.
        """
        if partitioned_fields and 'fields' in values:
            valid_fields = {field_key.name for field_key in values['fields']}
            actual_fields = set(partitioned_fields)
            invalid_fields = actual_fields - valid_fields
            if invalid_fields:
                raise ValueError(f'Value/s {invalid_fields} must be a subset of the "fields" field')
        return partitioned_fields

    @validator('data_mappings')
    def data_mappings_key_is_unique(cls, data_mappings):
        """
        Ensures that data mapping keys are unique.
        @param data_mappings:   The value of the field being validated.
        @param values:          All current object values
        @return:                Returns the field value being validated, or throws an exception on validation failure.
        """
        if data_mappings:
            fields = [mapping.key for mapping in data_mappings]
            if len(set(fields)) != len(fields):
                raise ValueError('Data Mapping keys must be unique.')
        return data_mappings

    @validator('data_mappings')
    def target_column_name_must_exist_in_table_definition(cls, data_mappings, values):
        """
        Validates that any column name also exists in a table definition.
        @param data_mappings:   The value of the field being validated.
        @param values:          All current object values
        @return:                Returns the field value being validated, or throws an exception on validation failure.
        """
        if data_mappings and 'fields' in values:
            fields = values['fields']
            table_definition_fields = {field_dto.name for field_dto in fields}
            for data_mapping in data_mappings:
                trans_exp_fields = {trans_exp.target_column_name for trans_exp in data_mapping.transformation_expressions}
                invalid_fields = trans_exp_fields - table_definition_fields
                if invalid_fields:
                    raise ValueError(f'Value/s {invalid_fields} of data mapping [{data_mapping.key}] must be a subset of the table_definition "fields" field')
        return data_mappings

    @staticmethod
    def from_business_object(table_name: str, src_object: TableDefinition, data_mapping_dtos: [DataMappingDTO]):
        """
        Acts as a factory method to generate a DTO from a business-logic-layer object.
        @param data_mapping: The business object to convert from.
        @return:    A new instance of the DTO based on the business object.
        """

        field_dtos = [FieldDTO.from_business_object(field, src_object.primary_keys) for field in src_object.fields.values()]

        db_name = ''
        table_parts = table_name.split('.')
        table_name = table_parts.pop()

        if table_parts:
            db_name = table_parts.pop()

        return TableDefinitionDTO(
            database_name=db_name,
            table_name=table_name,
            table_business_name=src_object.table_business_name,
            table_description=src_object.table_description,
            fields=field_dtos,
            partitioned_fields=list(src_object.partitioned_fields.keys()),
            primary_keys=src_object.primary_keys,
            data_mappings=data_mapping_dtos
         )

    def to_business_object(self) -> TableDefinition:
        """
        Converts the DTO to a business object to be used in business logic.
        @return:    A new instance of business object based on the DTO.
        """

        # Convert fields DTO list to a dictionary of business objects.
        fields = {fieldDTO.name: fieldDTO.to_business_object() for fieldDTO in self.fields}
        partitioned_fields = {name: fields for name, field in fields.items() if name in self.partitioned_fields}
        primary_keys = [field_dto.name for field_dto in self.fields if field_dto.primary_key]

        return TableDefinition(
            self.database_name,
            self.qualified_table_name(),
            self.table_business_name,
            self.table_description,
            fields,
            partitioned_fields,
            primary_keys
        )

    def qualified_table_name(self) -> str:
        """
        The business object table_name is fully qualified and includes the DB name.
        @return: A string representing the fully qualified table name.
        """

        if self.database_name:
            return self.database_name + "." + self.table_name
        else:
            return self.table_name
