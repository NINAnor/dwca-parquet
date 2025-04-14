from typing import Annotated

import duckdb
import fsspec
from fastapi import Depends
from fastapi.templating import Jinja2Templates

from .settings import Settings, conn, fs, s3fs, settings, templates


def get_settings():
    return settings


def get_local_fs():
    return fs


def duckdb_connection():
    return conn.cursor()


def get_templates():
    return templates


def get_s3fs():
    return s3fs


SettingsDep = Annotated[Settings, Depends(get_settings)]
LocalFsDep = Annotated[fsspec.AbstractFileSystem, Depends(get_local_fs)]
DBDep = Annotated[duckdb.DuckDBPyConnection, Depends(duckdb_connection)]
TemplatesDep = Annotated[Jinja2Templates, Depends(get_templates)]
S3FsDep = Annotated[fsspec.AbstractFileSystem, Depends(get_s3fs)]
