import uvicorn

from fastapi import FastAPI

from transform_generator.lib.logging import get_logger
from transform_generator.api.routers import config_router
from transform_generator.api.routers import generate_sql_router

app = FastAPI()
logger = get_logger(__name__)

app.include_router(config_router.router)
app.include_router(generate_sql_router.router)

# Run the server only if called via python command line (not module inclusion).
# Swagger docs viewable at http://<host>:8001/docs
if __name__ == "__main__":
    uvicorn.run(app)
