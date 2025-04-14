import pathlib

import duckdb
import fsspec
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings, SettingsConfigDict
from s3fs import S3FileSystem


class Settings(BaseSettings):
    ipt_public: str
    resource_folder: str = "resources"
    connection: str = ":memory:"
    s3_prefix: str = "/"
    s3_url_style: str = "path"
    s3_bucket: str
    aws_secret_key: str
    aws_endpoint_url: str
    aws_access_key: str

    model_config = SettingsConfigDict(env_file=".env")


templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "templates")


settings = Settings()

fs = fsspec.filesystem("file")
s3fs = S3FileSystem(endpoint_url=settings.aws_endpoint_url, anon=True)

conn = duckdb.connect(settings.connection)


def duckdb_install_extensions():
    print(
        "install extensions",
        conn.execute("""
        INSTALL zipfs FROM community;
        INSTALL spatial;
    """).fetchall(),
    )


def duckdb_load_extensions():
    print(
        "load extensions",
        conn.execute("""
        LOAD zipfs;
        LOAD spatial;
        LOAD httpfs;
    """).fetchall(),
    )


def duckdb_load_s3_credentials():
    print(
        "load secrets",
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
    """).fetchall(),
    )
