import pathlib

import xmltodict
from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from ..dependencies import DBDep, LocalFsDep, S3FsDep, SettingsDep, TemplatesDep
from ..libs.dwca import get_context_from_metafile

router = APIRouter()


@router.get("/resources/")
def get_resources(settings: SettingsDep, fs: LocalFsDep):
    result = fs.ls(settings.resource_folder, detail=True)
    response = {"resources": []}
    for e in result:
        response["resources"].append({"id": pathlib.Path(e["name"]).name})

    return response


@router.get("/resources/{resource_id}/")
def get_resource(resource_id: str, fs: LocalFsDep, settings: SettingsDep):
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


@router.get("/resources/{resource_id}/latest.parquet")
def get_resource_as_latest_parquet(
    resource_id: str,
    settings: SettingsDep,
    fs: LocalFsDep,
):
    resource = fs.open(
        pathlib.Path(settings.resource_folder) / resource_id / "resource.xml"
    )
    meta = xmltodict.parse(resource)
    version_id = meta["resource"]["versionHistory"]["versionhistory"][0]["version"]
    return RedirectResponse(f"v{version_id}.parquet")


@router.get("/resources/{resource_id}/v{version_id}.parquet")
def get_resource_as_parquet(
    resource_id: str,
    version_id: str,
    settings: SettingsDep,
    conn: DBDep,
    templates: TemplatesDep,
    s3fs: S3FsDep,
):
    destination_path = (
        pathlib.Path(settings.resource_folder)
        / resource_id
        / f"dwca-v{version_id}.parquet"
    )

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{destination_path}"

    if not s3fs.exists(s3_path):
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

        ctx = get_context_from_metafile(resource_path=resource_path)

        query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
        cursor.sql(query).write_parquet(s3_path)

    public_url = f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{destination_path}"
    return RedirectResponse(public_url)
