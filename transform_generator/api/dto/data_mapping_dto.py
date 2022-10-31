from pydantic import BaseModel, conlist, constr, validator

from transform_generator.generator.transform_rule import TransformRule
from transform_generator.lib.data_mapping import DataMapping
from transform_generator.api.dto.transform_exp_dto import TransformExpDTO


# A DTO which is used to map API requests/responses to the DataMapping business object.
from transform_generator.lib.mapping import get_asts_by_target_column


class DataMappingDTO(BaseModel):
    key: constr(min_length=1, strict=True)
    transformation_expressions: conlist(TransformExpDTO, min_items=1)

    @staticmethod
    def from_business_object(instance_key: str, src_object: DataMapping):
        """
        Acts as a factory method to generate a DTO from a business-logic-layer object.
        @param instance_key: A string to be used as a unique identifier for the DTO
        @param data_mapping: The business object to convert from.
        @return:    A new instance of the DTO based on the business object.
        """

        # Create transformations expression based on target columns.
        ast_dto_object = [TransformExpDTO.from_business_object(src_object.ast(target_column_name), target_column_name)
                          for target_column_name in src_object.target_column_names]

        return DataMappingDTO(key=instance_key,
                              transformation_expressions=ast_dto_object
                              )

    def to_business_object(self, parent_table_definition_dto) -> DataMapping:
        """
        Converts the DTO to a business object to be used in business logic.
        @return:    A new instance of DataMapping based on the DTO.
        """
        tbl_def = parent_table_definition_dto
        fields = {field.name: field for field in tbl_def.fields}

        transform_rules = []

        for transform_exp_dto in self.transformation_expressions:
            tgt_column_name = transform_exp_dto.target_column_name
            tgt_data_type = fields[tgt_column_name].data_type

            transform_rule = TransformRule('',
                                           '',
                                           '',
                                           '',
                                           tbl_def.database_name,
                                           tbl_def.table_name,
                                           tgt_column_name,
                                           tgt_data_type,
                                           transform_exp_dto.expression)
            transform_rules.append(transform_rule)

        ast_by_target_column_names = get_asts_by_target_column(transform_rules)

        return DataMapping(
            self.key,
            parent_table_definition_dto.database_name,
            parent_table_definition_dto.table_name,
            set(ast_by_target_column_names.keys()),
            ast_by_target_column_names
        )
