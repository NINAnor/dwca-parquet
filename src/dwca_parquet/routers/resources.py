import re

import xmltodict
from fastapi import APIRouter, Request

from ..dependencies import (
    LocalFsDep,
    QueueDep,
    SettingsDep,
)
from ..libs.csw import eml_to_records
from ..libs.ipt import get_dataset_metadata, get_datasets
from ..libs.parquet import version_to_parquet

router = APIRouter()


@router.get("/resources")
async def get_resources(settings: SettingsDep, fs: LocalFsDep, request: Request):
    response = {"resources": []}

    async for resource in get_datasets(request.base_url, settings.ipt_public):
        response["resources"].append(resource)

    return response


@router.post("/resources/csw")
async def generate_csw(q: QueueDep, settings: SettingsDep):
    q.enqueue(eml_to_records)
    return {
        "result": f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.csw_path}"
    }


@router.get("/resources/{resource_id}")
async def get_resource(resource_id: str, settings: SettingsDep, q: QueueDep):
    response = {
        "id": resource_id,
        "ipt_url": settings.ipt_public + "/resource?r=" + resource_id,
        "ipt_eml": settings.ipt_public + "/eml.do?r=" + resource_id,
        "ipt_dwca": settings.ipt_public + "/archive.do?r=" + resource_id,
    }

    text = get_dataset_metadata(settings.ipt_public, resource_id)
    response["meta"] = xmltodict.parse(text)
    response["version"] = (
        response["meta"]["eml:eml"]["@packageId"].split("/")[1].replace("v", "")
    )

    # parquet handling
    s3_path = (
        f"s3://{settings.s3_bucket}{settings.resources_prefix}{resource_id}.parquet"
    )

    response["parquet_url"] = (
        f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.resources_prefix}{resource_id}.parquet"  # noqa: E501
    )
    response["s3_path"] = (
        s3_path
        + f"?s3_endpoint={re.sub(r'https?://', '', settings.aws_endpoint_url)}&s3_url_style={settings.s3_url_style}"  # noqa: E501
    )

    q.enqueue(version_to_parquet, resource_id, response["version"])

    return response
