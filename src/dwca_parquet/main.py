import logging

from fastapi import FastAPI

from .routers import resources
from .settings import settings

logging.info(settings)

app = FastAPI(root_path="/api/v1")

app.include_router(resources.router)
