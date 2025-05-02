from typing import Annotated

import duckdb
import fsspec
from fastapi import Depends
from fastapi.templating import Jinja2Templates
from redis import Redis
from rq import Queue

from .settings import Settings, fs, get_connection, s3fs, settings, templates


def get_settings():
    return settings


def get_local_fs():
    return fs


def duckdb_connection():
    return get_connection().cursor()


def get_templates():
    return templates


def get_s3fs():
    return s3fs


def get_queue():
    return Queue(connection=Redis.from_url(settings.redis_url))


SettingsDep = Annotated[Settings, Depends(get_settings)]
LocalFsDep = Annotated[fsspec.AbstractFileSystem, Depends(get_local_fs)]
DBDep = Annotated[duckdb.DuckDBPyConnection, Depends(duckdb_connection)]
TemplatesDep = Annotated[Jinja2Templates, Depends(get_templates)]
S3FsDep = Annotated[fsspec.AbstractFileSystem, Depends(get_s3fs)]
QueueDep = Annotated[Queue, Depends(get_queue)]
