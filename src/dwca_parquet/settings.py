import logging
import pathlib

import duckdb
import fsspec
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings, SettingsConfigDict
from s3fs import S3FileSystem


class Settings(BaseSettings):
    ipt_public: str
    cache_path: str = ".dwca_cache/"
    connection: str = ":memory:"
    resources_prefix: str = "/ipt/datasets/"
    geoapi_path: str = "/geoapi/ipt-resources.json"
    csw_path: str = "/csw/ipt-metadata.parquet"
    s3_url_style: str = "path"
    s3_bucket: str
    aws_secret_key: str
    aws_endpoint_url: str
    aws_access_key: str
    redis_url: str

    model_config = SettingsConfigDict(env_file=".env")


templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "templates")


settings = Settings()

fs = fsspec.filesystem("file")
s3fs = S3FileSystem(endpoint_url=settings.aws_endpoint_url, anon=True)


def get_connection():
    return duckdb.connect(settings.connection)


if not pathlib.Path(settings.cache_path).exists():
    logging.info("cache folder not found, try creating it")
    pathlib.Path(settings.cache_path).mkdir(parents=True)


def duckdb_install_extensions():
    conn = get_connection()
    logging.info("install extensions")
    conn.execute("""
        INSTALL zipfs FROM community;
        INSTALL spatial;
        INSTALL httpfs;
    """).fetchall()


def duckdb_load_extensions(conn):
    logging.info("load extensions")
    conn.execute("""
        LOAD zipfs;
        LOAD spatial;
        LOAD httpfs;
    """).fetchall()


def duckdb_load_s3_credentials(conn):
    logging.info(
        "load secrets",
    )
    conn.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN 'env;config',
            REGION 'eu-west-1',
            KEY_ID '{settings.aws_access_key}',
            SECRET '{settings.aws_secret_key}',
            ENDPOINT '{settings.aws_endpoint_url.replace(r"https://", "")}',
            URL_STYLE '{settings.s3_url_style}'
        );
    """).fetchall()
