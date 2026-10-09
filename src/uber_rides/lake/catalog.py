"""Iceberg catalog factory.

* ``sql`` (default): pyiceberg's SQL catalog on SQLite, the local stand-in for the AWS Glue Data
  Catalog. Warehouse on the local filesystem or on (emulated) S3.
* ``glue``: pyiceberg's Glue catalog. Used against real AWS, or against moto's Glue in tests.

FileIO is fsspec-based for both: it handles ``file://`` URIs on Windows and ``s3://`` with a
custom endpoint (moto / LocalStack).
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote, urlparse

from pyiceberg.catalog import Catalog

from uber_rides.config import Settings


def catalog_properties(settings: Settings) -> dict[str, str]:
    props = {
        "warehouse": settings.warehouse_uri,
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.region": settings.aws_region,
    }
    if settings.s3_endpoint_url:
        props["s3.endpoint"] = settings.s3_endpoint_url
    return props


def get_catalog(settings: Settings) -> Catalog:
    props = catalog_properties(settings)
    if settings.catalog_type == "glue":
        from pyiceberg.catalog.glue import GlueCatalog

        glue_props = {**props, "glue.region": settings.aws_region}
        if settings.aws_endpoint_url:
            glue_props["glue.endpoint"] = settings.aws_endpoint_url
        return GlueCatalog(settings.catalog_name, **glue_props)

    from pyiceberg.catalog.sql import SqlCatalog

    uri = settings.catalog_db_uri
    if uri.startswith("sqlite:///"):
        Path(uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    if settings.warehouse_uri.startswith("file:"):
        local_path(settings.warehouse_uri).mkdir(parents=True, exist_ok=True)
    return SqlCatalog(settings.catalog_name, uri=uri, **props)


def local_path(uri: str) -> Path:
    """``file:///C:/x/y`` or ``file:///x/y`` -> a local Path. Other strings pass through."""
    if not uri.startswith("file:"):
        return Path(uri)
    parsed = urlparse(uri)
    path = unquote(parsed.path)
    if len(path) >= 3 and path[0] == "/" and path[2] == ":":  # /C:/...
        path = path[1:]
    return Path(path)
