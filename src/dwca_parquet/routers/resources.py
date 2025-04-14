import logging
import pathlib

import xmltodict
from fastapi import APIRouter
from fastapi.responses import FileResponse

from ..dependencies import DBDep, LocalFsDep, SettingsDep, TemplatesDep
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


@router.get("/resources/{resource_id}/{version_id}.parquet")
def get_resource_as_parquet(
    resource_id: str,
    version_id: str,
    settings: SettingsDep,
    conn: DBDep,
    templates: TemplatesDep,
):
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

        ctx = get_context_from_metafile(resource_path=resource_path)

        params = {
            **ctx,
            "destination": destination_path,
        }

        query = templates.get_template("query.sql").render(**params, trim_blocks=True)
        logging.debug(query)
        cursor.execute(query)

    return FileResponse(
        destination_path,
        media_type="application/vnd.apache.parquet",
        filename=f"{resource_id}-v{version_id}.parquet",
    )
