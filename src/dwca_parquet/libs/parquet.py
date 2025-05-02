import logging
import pathlib

import fsspec

from ..settings import (
    duckdb_load_extensions,
    duckdb_load_s3_credentials,
    get_connection,
    s3fs,
    settings,
    templates,
)
from .dwca import get_context_from_metafile

logger = logging.getLogger(__name__)


def version_to_parquet(resource_id: str, version_id: str):
    conn = get_connection()
    duckdb_load_extensions(conn)
    duckdb_load_s3_credentials(conn)
    logger.info(f"starting {resource_id}@{version_id}")
    base_path_versioned = f"{resource_id}/v{version_id}"

    s3_path = f"s3://{settings.s3_bucket}{settings.resources_prefix}{base_path_versioned}.parquet"
    s3_path_latest = (
        f"s3://{settings.s3_bucket}{settings.resources_prefix}{resource_id}.parquet"
    )
    cache = pathlib.Path(settings.cache_path) / f"{resource_id}-v{version_id}.zip"

    # Check that the version exists, otherwise create it and overwrite the latest one
    if not s3fs.exists(s3_path):
        try:
            # create a temporary cache to allow duckdb to read it
            # httpfs + zipfs does not work greatly together
            logger.info("downloading locally")
            with fsspec.open(
                f"{settings.ipt_public}/archive.do?r={resource_id}&v={version_id}"
            ) as source:
                with cache.open("wb") as dest:
                    dest.write(source.read())

            cursor = conn.cursor()
            ctx = get_context_from_metafile(resource_path=cache)
            query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
            logger.info("write to parquet")
            cursor.sql(query).write_parquet(s3_path, compression="zstd", overwrite=True)
            cursor.sql(query).write_parquet(
                s3_path_latest, compression="zstd", overwrite=True
            )
        finally:
            logger.info("done")
            cache.unlink(missing_ok=True)
    else:
        logger.info("already available")
