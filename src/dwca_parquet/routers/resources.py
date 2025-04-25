import pathlib
import re

import aiohttp
import fsspec
import xmltodict
from bs4 import BeautifulSoup
from fastapi import APIRouter, Request

from ..dependencies import (
    DBDep,
    LocalFsDep,
    S3FsDep,
    SettingsDep,
    templates,
)
from ..libs.dwca import get_context_from_metafile

router = APIRouter()


@router.get("/resources")
async def get_resources(settings: SettingsDep, fs: LocalFsDep, request: Request):
    response = {"resources": []}

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{settings.ipt_public}/rss") as res:
            text = await res.text()
            soup = BeautifulSoup(text, features="lxml-xml")
            for item in soup.find_all("item"):
                content = {
                    k.replace(":", "_"): v
                    for k, v in xmltodict.parse(item.prettify())["item"].items()
                }
                resource_id = content["link"].split("=")[1]
                response["resources"].append(
                    {
                        **content,
                        "id": resource_id,
                        "version": content["guid"]["#text"]
                        .split("/")[1]
                        .replace("v", ""),
                        "url": f"{request.base_url}api/resources/{resource_id}",
                    }
                )

    return response


@router.get("/resources/{resource_id}")
async def get_resource(
    resource_id: str,
    settings: SettingsDep,
    request: Request,
):
    response = {
        "id": resource_id,
        "ipt_url": settings.ipt_public + "/resource?r=" + resource_id,
        "ipt_eml": settings.ipt_public + "/eml.do?r=" + resource_id,
        "ipt_dwca": settings.ipt_public + "/archive.do?r=" + resource_id,
    }

    eml_file = fsspec.open(response["ipt_eml"], "rt")
    with eml_file as metadata:
        text = metadata.read()
        response["meta"] = xmltodict.parse(text)
        response["version"] = (
            response["meta"]["eml:eml"]["@packageId"].split("/")[1].replace("v", "")
        )
        response["ipt_dwca"] = (
            f"{request.base_url}api/resources/{resource_id}/v{response['version']}.zip"
        )

    response["generate_parquet_url"] = (
        f"{request.base_url}api/resources/{resource_id}/v{response['version']}/generate-parquet"
    )

    response["parquet_url"] = (
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{resource_id}-v{response['version']}.parquet"
    )

    return response


def version_to_parquet(conn, source_path, destination):
    cursor = conn.cursor()
    ctx = get_context_from_metafile(resource_path=source_path)
    query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
    cursor.sql(query).write_parquet(destination, compression="zstd", overwrite=True)


@router.get("/resources/{resource_id}/v{version_id}/generate-parquet")
def get_resource_as_parquet(
    resource_id: str,
    version_id: str,
    settings: SettingsDep,
    conn: DBDep,
    s3fs: S3FsDep,
):
    base_path = f"{resource_id}-v{version_id}"

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"

    if not s3fs.exists(s3_path):
        cache = pathlib.Path(settings.cache_path) / f"{resource_id}-v{version_id}.zip"

        # create a temporary cache to allow duckdb to read it
        # httpfs + zipfs does not work greatly together
        with fsspec.open(
            f"{settings.ipt_public}/archive.do?r={resource_id}&v={version_id}"
        ) as source:
            with cache.open("wb") as dest:
                dest.write(source.read())

        version_to_parquet(
            conn=conn,
            source_path=cache,
            destination=s3_path,
        )

        cache.unlink(missing_ok=True)

    url = f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"  # noqa: E501

    return {
        "parquet_url": url,
        "s3_path": s3_path
        + f"?s3_endpoint={re.sub(r'https?://', '', settings.aws_endpoint_url)}&s3_url_style={settings.s3_url_style}",  # noqa: E501
    }
