from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Dict, List

from transform_generator.api.adapter import adapter
from transform_generator.api.dto.project_config_dto import ProjectConfigDTO
from transform_generator.api.dto.query_config import QueryConfigDTO
from transform_generator.generator.transform_gen import generate_select_query
from transform_generator.lib.logging import get_logger
from transform_generator.api.dto.table_definition_dto import TableDefinitionDTO
from transform_generator.api.dto.data_mapping_response_dto import DataMappingResponseDTO
from transform_generator.lib.project_config_entry import ProjectConfigEntry

logger = get_logger(__name__)

# Instantiate a router with the common route properties.
router = APIRouter(
    prefix="/generate",
    tags=["SQL"],
    responses={404: {"description": "Not found"}},
)

@router.post(
    "/sql-simple",
    tags=["SQL"],
    response_model=List[QueryConfigDTO],
    description="Generates SQL transformation queries for a given project configuration."
)
async def generate_sql_simple(table_definition_dtos: List[TableDefinitionDTO]):
    # Generate table definition dictionary as expected by business logic.
    query_configs = []

    for table_definition_index, table_definition_dto in enumerate(table_definition_dtos):
        loc = ["body", table_definition_index, "data_mappings"]
        query_config = queries_for_table_def(table_definition_dto=table_definition_dto, loc=loc)
        query_configs.append(query_config)


    return query_configs


@router.post(
    "/sql",
    tags=["SQL"],
    response_model=List[QueryConfigDTO],
    description="Generates SQL transformation queries for a given project configuration."
)
async def generate_sql(project_config_dto: ProjectConfigDTO):
    #: OpenAPI validation for well-formed JSON happens in FastAPI via Pydantic before we reach this point.
    logger.info("Received request to generate SQL.")

    # Use the incoming ProjectConfigDTO to generate config structures expected by business logic.
    config_entry_by_mapping_filename, \
        config_filename_by_target_table, \
        config_entry_by_config_filename, \
        database_variables, \
        project_config_entry_by_filename, \
        mappings_by_mapping_filename, \
        table_definitions_by_table_name = adapter.get_config_structures_from_dtos(project_config_dto)

    # Generate table definition dictionary as expected by business logic.
    query_configs = []

    for group_index, config_entry_group in enumerate(project_config_dto.config_entry_groups):
        for config_index, config_entry in enumerate(config_entry_group.config_entries):
            loc = ["body",
                   "config_entry_groups", group_index,
                   "config_entries", config_index,
                   "table_definition",
                   "data_mappings"
                ]
            queries = queries_for_table_def(
                    config_entry.table_definition,
                    config_entry.target_language.name,
                    project_config_entry_by_filename,
                    config_filename_by_target_table,
                    config_entry.load_type,
                    loc
                )

            query_configs.append(
                queries
            )
    return query_configs


def queries_for_table_def(
        table_definition_dto: TableDefinitionDTO,
        language: str = 'DATABRICKS',
        project_config_entry_by_filename: Dict[str, ProjectConfigEntry] = {},
        config_filename_by_target_table: Dict[str, str] = {},
        load_type: str = 'full',
        loc = {}
):
    """
    Generates queries for a target table definition.
    @param table_definition_dto:                The incoming table defintiion
    @param language:                            Output language
    @param project_config_entry_by_filename:  Dict of generator config entries.
    @param config_filename_by_target_table:     Used for compatibility to generate_sql_query.
    @param load_type:                           full/incremental
    @param loc:                                 Parent location of the table_definitions, for error reporting.
    @return:
    """

    # Validate to ensure that a table_definition has at least one data_mapping.
    # NOTE: This validation does not happen in the DTO definition as this rule depends on the use-case.
    if not table_definition_dto.data_mappings:
        raise HTTPException(
            status_code=422,
            detail=jsonable_encoder(
                [{"msg": "table_definitions must contain at least one data_mapping for SQL generation.", "loc": loc}]
            )
        )

    data_mapping_responses = []
    table_definition_bo = table_definition_dto.to_business_object()

    for data_mapping_dto in table_definition_dto.data_mappings:
        data_mapping = data_mapping_dto.to_business_object(table_definition_dto)
        select_query = generate_select_query(
            data_mapping,
            table_definition_bo,
            language,
            project_config_entry_by_filename,
            config_filename_by_target_table,
            load_type
        )
        data_mapping_responses.append(
            DataMappingResponseDTO(
                data_mapping_key=data_mapping_dto.key,
                query=select_query
            )
        )

    # One query config is returned for each target table; with each data mapping resolving to a query.
    query_config = QueryConfigDTO(
        target_table=data_mapping.table_name,
        data_mapping_responses=data_mapping_responses
    )
    return query_config
