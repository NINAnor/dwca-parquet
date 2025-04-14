import logging

from fastapi import FastAPI

from .routers import resources
from .settings import duckdb_load_extensions, settings

duckdb_load_extensions()

logging.info(settings)

app = FastAPI()

app.include_router(resources.router, prefix="/api")
