import logging

from fastapi import FastAPI

from .routers import resources
from .settings import duckdb_load_extensions, duckdb_load_s3_credentials, settings

duckdb_load_extensions()
duckdb_load_s3_credentials()
logging.info(settings)

app = FastAPI(root_path="/api/v1")

app.include_router(resources.router)
