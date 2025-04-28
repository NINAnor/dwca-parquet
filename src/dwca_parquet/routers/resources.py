import logging
import pathlib
import re

import aiohttp
import fsspec
import xmltodict
from bs4 import BeautifulSoup
from fastapi import APIRouter, BackgroundTasks, Request

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
                        "url": f"{request.base_url}/resources/{resource_id}",
                    }
                )

    return response


@router.get("/resources/{resource_id}")
async def get_resource(
    resource_id: str,
    settings: SettingsDep,
    request: Request,
    conn: DBDep,
    s3fs: S3FsDep,
    background_tasks: BackgroundTasks,
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

    # parquet handling
    base_path = f"{resource_id}-v{response['version']}"

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"

    response["parquet_url"] = (
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"  # noqa: E501
    )
    response["s3_path"] = (
        s3_path
        + f"?s3_endpoint={re.sub(r'https?://', '', settings.aws_endpoint_url)}&s3_url_style={settings.s3_url_style}"  # noqa: E501
    )

    background_tasks.add_task(
        version_to_parquet, conn, settings, s3fs, resource_id, response["version"]
    )

    return response


def version_to_parquet(conn, settings, s3fs, resource_id: str, version_id: str):
    logging.info(f"starting {resource_id}@{version_id}")
    base_path = f"{resource_id}-v{version_id}"

    s3_path = f"s3://{settings.s3_bucket}{settings.s3_prefix}{base_path}.parquet"
    cache = pathlib.Path(settings.cache_path) / f"{resource_id}-v{version_id}.zip"

    if not s3fs.exists(s3_path):
        try:
            # create a temporary cache to allow duckdb to read it
            # httpfs + zipfs does not work greatly together
            logging.info("downloading locally")
            with fsspec.open(
                f"{settings.ipt_public}/archive.do?r={resource_id}&v={version_id}"
            ) as source:
                with cache.open("wb") as dest:
                    dest.write(source.read())

            cursor = conn.cursor()
            ctx = get_context_from_metafile(resource_path=cache)
            query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
            logging.info("write to parquet")
            cursor.sql(query).write_parquet(s3_path, compression="zstd", overwrite=True)
        finally:
            logging.info("done")
            cache.unlink(missing_ok=True)
    else:
        logging.info("already available")
