from fastapi import APIRouter

from transform_generator.api.dto.project_config_dto import ProjectConfigDTO
from transform_generator.lib.logging import get_logger

logger = get_logger(__name__)

# Instantiate a router with the common route properties.
router = APIRouter(
    prefix="/config",
    tags=["Config"],
    responses={404: {"description": "Not found"}},
)


@router.post(
    "/validate",
    tags=["Config"],
    response_model=ProjectConfigDTO,
    description="Parses and validates uploaded configuration models."
)
async def config_validate(config: ProjectConfigDTO):
    #: OpenAPI validation for well-formed JSON happens in FastAPI before we reach this point.
    logger.info("Received request to validate configuration.")

    return config
