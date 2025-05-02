import json
import logging

import pyarrow as pa
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema
from pygeometa.schemas.iso19139 import ISO19139OutputSchema
from shapely.geometry import box

from ..settings import (
    duckdb_load_extensions,
    duckdb_load_s3_credentials,
    get_connection,
    settings,
)
from .ipt import get_dataset_metadata, get_datasets

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()
iso = ISO19139OutputSchema()

logger = logging.getLogger(__name__)


def get_anytext(bag):
    """
    generate bag of text for free text searches
    accepts list of words, string of XML, or etree.Element
    """

    if isinstance(bag, list):  # list of words
        return " ".join([_f for _f in bag if _f]).strip()
    else:  # xml
        if isinstance(bag, bytes) or isinstance(bag, str):
            # serialize to lxml
            bag = etree.fromstring(bag, PARSER)  # noqa: S320
        # get all XML element content
        return " ".join([value.strip() for value in bag.xpath("//text()")])


def eml_to_records():
    rows = []

    for ds in get_datasets("", settings.ipt_public):
        text = get_dataset_metadata(ipt_url=settings.ipt_public, resource_id=ds["id"])
        metadata = eml.import_(text)

        xml = iso.write(metadata)
        fts = get_anytext(xml)
        idf = metadata["identification"]
        bbox = idf["extents"]["spatial"][0]["bbox"]

        contribs = []
        for role, contact in metadata["contact"].items():
            role = role.split("_")[0]
            contribs.append(contact["individualname"])

        keywords = []
        for _k, v in idf["keywords"].items():
            keywords += v["keywords"]

        links = [
            {
                "name": "Parquet",
                "description": "The resource as (geo)parquet file",
                "protocol": "FILE:GEO",
                "url": f"{settings.aws_endpoint_url}/{settings.s3_bucket}{settings.resources_prefix}{ds['id']}.parquet",  # noqa: E501
            },
            {
                "name": "DWCA",
                "description": "The resource as Darwin Core Archive",
                "protocol": "file",
                "url": f"{settings.ipt_public}/archive.do?r={ds['id']}",  # noqa: E501
            },
        ]

        rows.append(
            {
                "identifier": metadata["metadata"]["identifier"],
                "typename": "gmd:MD_Metadata",
                "schema": "http://www.isotc211.org/2005/gmd",
                "mdsource": "local",
                "insert_date": idf["dates"]["publication"],
                "title": ds["title"],
                "date_modified": idf["dates"]["publication"],
                "type": "service",
                "format": None,
                "wkt_geometry": box(*bbox).wkt,
                "metadata": xml,
                "xml": xml,
                "keywords": ", ".join(set(keywords)),
                "metadata_type": "application/xml",
                "anytext": fts,
                "abstract": metadata["identification"]["abstract"],
                "date": idf["dates"]["publication"],
                "creator": "Norsk institutt for naturforskning (NINA)",
                "publisher": "Norsk institutt for naturforskning (NINA)",
                "contributor": "; ".join(set(contribs)),
                "links": json.dumps(links),
            }
        )

    logger.info("converting to arrow")
    records = pa.Table.from_pylist(rows)  # noqa: F841
    conn = get_connection()
    duckdb_load_extensions(conn)
    duckdb_load_s3_credentials(conn)
    logger.info("write to S3")
    conn.sql("from records").write_parquet(
        f"s3://{settings.s3_bucket}{settings.csw_path}",
        compression="zstd",
        overwrite=True,
    )
