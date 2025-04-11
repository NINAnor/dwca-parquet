from fastapi import FastAPI
from pydantic_settings import BaseSettings
import duckdb
import fsspec
import pathlib
import xmltodict
import re
from bs4 import BeautifulSoup
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
import logging


class Settings(BaseSettings):
    resource_folder: str = "resources"
    connection: str = ":memory:"


settings = Settings()
app = FastAPI()

templates = Jinja2Templates(directory=pathlib.Path(__file__).parent / "templates")

conn = duckdb.connect(settings.connection)
conn.execute("""
INSTALL zipfs FROM community;
INSTALL spatial;
LOAD spatial;
LOAD zipfs;
""")

fs = fsspec.filesystem("file")


@app.get("/resources/")
def get_resources():
    result = fs.ls(settings.resource_folder, detail=True)
    response = {"resources": []}
    for e in result:
        response["resources"].append({"id": pathlib.Path(e["name"]).name})

    return response


@app.get("/resources/{resource_id}/")
def get_resource(resource_id):
    response = {
        "id": resource_id,
        "versions": [],
    }

    resource = fs.open(
        pathlib.Path(settings.resource_folder) / resource_id / "resource.xml"
    )

    meta = xmltodict.parse(resource)
    response["meta"] = meta

    for version in meta["resource"]["versionHistory"]["versionhistory"]:
        response["versions"].append(
            {"id": version["version"], "date": version["released"]}
        )

    return response


class SourceLayer:
    def __init__(self, node, base_path, extension=False) -> None:
        self.type = pathlib.Path(node.find("location").text).stem

        self.path = f'zip://{base_path}/{node.find("location").text}'

        with fsspec.open(
            f'zip://{node.find("location").text}::{base_path}',
            encoding=node["encoding"],
            mode="r",
        ) as f:
            sep = re.compile(node["fieldsTerminatedBy"])
            headers = re.split(sep, f.readline().rstrip())

            # extensions nodes have a "coreid" fields that contains the id of "core" row, this is needed for the join
            id_field_lookup = "coreid" if extension else "id"
            self.id = headers[int(node.find(id_field_lookup)["index"])]

    def __str__(self) -> str:
        return self.type


@app.get("/resources/{resource_id}/{version_id}.parquet")
def get_resource_as_parquet(resource_id, version_id):
    destination_path = (
        pathlib.Path(settings.resource_folder)
        / resource_id
        / f"dwca-v{version_id}.parquet"
    )

    if not destination_path.exists():
        resource_path = (
            pathlib.Path(settings.resource_folder)
            / resource_id
            / f"dwca-v{version_id}.zip"
        )
        cursor = conn.cursor()

        destination_path = (
            pathlib.Path(settings.resource_folder)
            / resource_id
            / f"dwca-v{version_id}.parquet"
        )

        meta_file = fsspec.open(f"zip://meta.xml::{resource_path}")
        with meta_file as meta:
            soup = BeautifulSoup(meta, features="lxml-xml")
            extensions = []

            core = SourceLayer(soup.find("core"), resource_path)
            for extension in soup.find_all("extension"):
                extensions.append(SourceLayer(extension, resource_path, extension=True))

            ctx = {
                "core": core,
                "extensions": extensions,
                "destination": destination_path,
            }

            query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
            logging.debug(query)
            cursor.execute(query)

    return FileResponse(
        destination_path,
        media_type="application/vnd.apache.parquet",
        filename=f"{resource_id}-v{version_id}.parquet",
    )
