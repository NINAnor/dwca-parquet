import pathlib

import duckdb
import fsspec
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    resource_folder: str = "resources"
    connection: str = ":memory:"


templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "templates")


settings = Settings()

fs = fsspec.filesystem("file")

conn = duckdb.connect(settings.connection)


def duckdb_install_extensions():
    conn.execute("""
        INSTALL zipfs FROM community;
        INSTALL spatial;
    """)


def duckdb_load_extensions():
    conn.execute("""
        LOAD zipfs;
        LOAD spatial;
    """)
