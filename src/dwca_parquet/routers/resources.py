import logging
import pathlib

import xmltodict
from bs4 import BeautifulSoup
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from ..dependencies import (
    DBDep,
    LocalFsDep,
    S3FsDep,
    SettingsDep,
    templates,
)
from ..libs.dwca import get_context_from_metafile

router = APIRouter()


@router.get("/resources/")
def get_resources(settings: SettingsDep, fs: LocalFsDep, request: Request):
    result = fs.ls(settings.resource_folder, detail=True)
    response = {"resources": []}
    for e in result:
        resource_id = pathlib.Path(e["name"]).name
        try:
            meta = fs.open(
                pathlib.Path(settings.resource_folder) / resource_id / "eml.xml"
            )
            soup = BeautifulSoup(meta, features="lxml-xml")
            response["resources"].append(
                {
                    "id": resource_id,
                    "title": soup.find("title").text,
                    "url": f"{request.url}{resource_id}/",
                }
            )
        except FileNotFoundError:
            logging.warning(f"eml.xml not found for resource {resource_id}")

    return response


@router.get("/resources/{resource_id}/")
def get_resource(
    resource_id: str, fs: LocalFsDep, settings: SettingsDep, conn: DBDep, s3fs: S3FsDep
):
    response = {
        "id": resource_id,
        "ipt_url": settings.ipt_public + "/resource?r=" + resource_id,
        "versions": [],
    }

    resource = fs.open(
        pathlib.Path(settings.resource_folder) / resource_id / "resource.xml"
    )

    meta = xmltodict.parse(resource)
    print(meta)

    try:
        for version in meta["resource"]["versionHistory"]["versionhistory"]:
            response["versions"].append(
                {"id": version["version"], "date": version["released"]}
            )
    except Exception as e:
        logging.error(e)

    response["version"] = meta["resource"]["emlVersion"]

    metadata = fs.open(pathlib.Path(settings.resource_folder) / resource_id / "eml.xml")

    response["meta"] = xmltodict.parse(metadata)

    response["dwca_url"] = (
        settings.ipt_public + f"/archive.do?r={resource_id}&v={response['version']}"
    )

    base_path = (
        pathlib.Path(settings.resource_folder)
        / resource_id
        / f"dwca-v{response['version']}"
    )

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"

    if not s3fs.exists(s3_path):
        version_to_parquet(
            conn=conn, source_path=str(base_path) + ".zip", destination=s3_path
        )

    response["parquet_url"] = (
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"
    )

    return response


def version_to_parquet(conn, source_path, destination):
    cursor = conn.cursor()
    ctx = get_context_from_metafile(resource_path=source_path)
    query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
    cursor.sql(query).write_parquet(destination)


@router.get("/resources/{resource_id}/latest.parquet")
def get_resource_as_latest_parquet(
    resource_id: str,
    settings: SettingsDep,
    fs: LocalFsDep,
    conn: DBDep,
    s3fs: S3FsDep,
):
    resource = fs.open(
        pathlib.Path(settings.resource_folder) / resource_id / "resource.xml"
    )
    meta = xmltodict.parse(resource)
    version_id = meta["resource"]["versionHistory"]["versionhistory"][0]["version"]

    base_path = (
        pathlib.Path(settings.resource_folder) / resource_id / f"dwca-v{version_id}"
    )

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"

    if not s3fs.exists(s3_path):
        version_to_parquet(
            conn=conn, source_path=str(base_path) + ".zip", destination=s3_path
        )

    return RedirectResponse(
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"
    )


@router.get("/resources/{resource_id}/v{version_id}.parquet")
def get_resource_as_parquet(
    resource_id: str,
    version_id: str,
    settings: SettingsDep,
    conn: DBDep,
    s3fs: S3FsDep,
):
    base_path = (
        pathlib.Path(settings.resource_folder) / resource_id / f"dwca-v{version_id}"
    )

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"

    if not s3fs.exists(s3_path):
        version_to_parquet(
            conn=conn, source_path=str(base_path) + ".zip", destination=s3_path
        )

    return RedirectResponse(
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"
    )
