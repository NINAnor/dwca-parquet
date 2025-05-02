import logging

import pyarrow as pa
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema

from ..settings import (
    duckdb_load_extensions,
    duckdb_load_s3_credentials,
    get_connection,
    settings,
)
from .ipt import get_dataset_metadata, get_datasets

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()

logger = logging.getLogger(__name__)


def ipt_to_pygeoapi_resources():
    rows = []

    for ds in get_datasets("", settings.ipt_public):
        text = get_dataset_metadata(ipt_url=settings.ipt_public, resource_id=ds["id"])
        metadata = eml.import_(text)

        idf = metadata["identification"]
        spatial = idf["extents"]["spatial"][0]

        contribs = []
        for role, contact in metadata["contact"].items():
            role = role.split("_")[0]
            contribs.append(contact["individualname"])

        keywords = []
        for _k, v in idf["keywords"].items():
            keywords += v["keywords"]

        rows.append(
            {
                "id": metadata["metadata"]["identifier"],
                "type": "collection",
                "visibility": "default",
                "title": ds["title"],
                "extents": {"spatial": spatial},
                "keywords": list(set(keywords)),
                "description": metadata["identification"]["abstract"],
                "providers": [
                    {
                        "type": "feature",
                        "name": "Parquet",
                        "default": True,
                        "id_field": "fid",
                        "editable": False,
                        "storage_crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                        "data": f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.resources_prefix}{ds['id']}.parquet",  # noqa: E501
                    }
                ],
            }
        )

    logger.info("converting to arrow")
    records = pa.Table.from_pylist(rows)  # noqa: F841
    conn = get_connection()
    duckdb_load_extensions(conn)
    duckdb_load_s3_credentials(conn)
    logger.info("write to S3")
    conn.sql(f"""
        COPY records to 's3://{settings.s3_bucket}{settings.geoapi_path}' (FORMAT json, ARRAY true)
    """)  # noqa: E501
