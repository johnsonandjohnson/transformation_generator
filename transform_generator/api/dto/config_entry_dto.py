from pydantic import BaseModel, validator, constr, StrictStr, StrictBool, StrictInt
from typing import Optional
from enum import Enum
from transform_generator.api.dto.table_definition_dto import TableDefinitionDTO
from transform_generator.lib.config_entry import ConfigEntry

import logging
logger = logging.getLogger(__name__)


class TargetType(str, Enum):
    TABLE = 'table'
    VIEW = 'view'
    PROGRAM = 'program'
    LINEAGE = 'lineage'


class TargetLanguage(str, Enum):
    DATABRICKS = 'databricks'


class LoadTypes(str, Enum):
    FULL = 'full'
    INCREMENTAL = 'incremental'


# A DTO which is used to map API requests/responses to the ConfigEntry business object.
# The order of the fields in the BaseModel and order of validator decorators here matters.
# Pydantic validates fields according to the order they are listed, changing this order may break unit tests.
class ConfigEntryDTO(BaseModel):
    config_entry_group_key: constr(min_length=1, strict=True)
    target_type: TargetType
    target_language: TargetLanguage
    table_definition: TableDefinitionDTO
    dependencies: Optional[StrictStr]
    load_type: LoadTypes
    load_synapse: StrictBool
    sequence: Optional[StrictInt]

    @validator('dependencies')
    def empty_unless_target_type_program(cls, dependencies, values):
        """
        Validates that any column name also exists in a table definition.
        @param dependencies:        The value of the field being validated.
        @param values:              All current object values
        @return:    Returns the field value being validated, or throws an exception on validation failure.
        """
        group = values['config_entry_group_key']
        if dependencies and 'target_type' in values and values['target_type'] not in {'program', 'lineage'}:
            value = values['target_type']
            raise ValueError(f'Must be empty unless target_type is program. Value was [{dependencies}] for target_type '
                             f'[{value}] in group [{group}].')
        return dependencies

    @staticmethod
    def from_business_object(
        config_entry_group_key: str,
        src_object: ConfigEntry,
        table_definition: TableDefinitionDTO
    ):
        """
        Acts as a factory method to generate a DTO from a business-logic-layer object.
        @param data_mapping: The business object to convert from.
        @return:    A new instance of the DTO based on the business object.
        """
        dependencies = src_object.dependencies
        if src_object.target_type not in {'program', 'lineage'}:
            # Normalize dependencies DTO value to None if not valid for this target_type
            if dependencies:
                logger.error('Dependencies must not be set unless target type is "program" for  table [%s] to DTO. '
                             'Defaulting to None', src_object.target_table)
            dependencies = None

        load_type = src_object.load_type or 'full'
        if load_type not in {'full', 'incremental'}:
            logger.error('Cannot convert load type [%s] for table [%s] to DTO. Defaulting to "full".',
                         src_object.load_type, src_object.target_table)

        target_type = src_object.target_type or 'table'
        target_language = src_object.target_language or 'databricks'
        sequence = src_object.sequence or None

        return ConfigEntryDTO(
            config_entry_group_key=config_entry_group_key,
            target_type=TargetType[target_type.upper()],
            target_language=TargetLanguage[target_language.upper()],
            table_definition=table_definition,
            dependencies=dependencies,
            load_type=LoadTypes[load_type.upper()],
            load_synapse=src_object.load_synapse,
            sequence=sequence
        )

    def to_business_object(self) -> ConfigEntry:
        """
        Converts the DTO to a business object to be used in business logic.
        @return:    A new instance of business object based on the DTO.
        """

        input_files_str = ",".join([data_mapping.key for data_mapping in self.table_definition.data_mappings])
        qualified_table_name = self.table_definition.database_name + "." + self.table_definition.table_name

        return ConfigEntry(
            self.config_entry_group_key,
            qualified_table_name,
            self.target_type.name,
            self.target_language.name,
            input_files_str.split(','),
            self.dependencies,
            [],
            self.load_type.name,
            self.load_synapse,
            self.sequence
        )
