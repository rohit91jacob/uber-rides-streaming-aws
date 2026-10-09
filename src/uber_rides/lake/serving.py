"""Expose the Iceberg tables to DuckDB (the local stand-in for Athena reading the Glue catalog).

``publish`` resolves each table's *current snapshot* through the Iceberg catalog and creates a
DuckDB view over exactly that snapshot's data files in schema ``lake`` of the serving database.
dbt then reads ``lake.*`` as sources. Readers therefore see committed snapshots only, never a
half-written load, which is the property Athena gets from the Glue catalog on AWS.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog, local_path
from uber_rides.lake.tables import ALL_TABLES, TableDef
from uber_rides.logs import get_logger

log = get_logger(__name__)


class SnapshotError(RuntimeError):
    """A snapshot uses delete files, which the file-list view cannot apply."""


def snapshot_files(table: Table) -> list[str]:
    files = []
    for task in table.scan().plan_files():
        if task.delete_files:
            raise SnapshotError(
                f"{table.name()} has delete files; rewrite it (copy-on-write) before publishing"
            )
        path = task.file.file_path
        files.append(local_path(path).as_posix() if path.startswith("file:") else path)
    return sorted(files)


def _sql_list(paths: list[str]) -> str:
    return "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"


def connect(settings: Settings, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = settings.serving_db
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    if not read_only:
        con.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
    return con


def _publish_one(con: duckdb.DuckDBPyConnection, settings: Settings, tdef: TableDef) -> int | None:
    try:
        table = get_catalog(settings).load_table(tdef.identifier(settings))
    except NoSuchTableError:
        return None
    name = f"lake.{tdef.name}"
    files = snapshot_files(table)
    kind = con.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_schema = 'lake' AND table_name = ?",
        [tdef.name],
    ).fetchone()
    if kind:
        con.execute(f"DROP {'VIEW' if kind[0] == 'VIEW' else 'TABLE'} {name}")
    if files:
        columns = ", ".join(f'"{c}"' for c in tdef.column_names)
        con.execute(
            f"CREATE VIEW {name} AS SELECT {columns} FROM read_parquet({_sql_list(files)}, union_by_name = true)"
        )
    else:  # empty table: keep the typed schema so dbt sources still resolve
        empty = tdef.arrow_schema.empty_table()
        con.register("_empty", empty)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _empty")
        con.unregister("_empty")
    return len(files)


def publish(settings: Settings) -> dict[str, int]:
    """(Re)create ``lake.<table>`` in the serving DuckDB for every Iceberg table that exists."""
    con = connect(settings)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS lake")
        published = {}
        for tdef in ALL_TABLES:
            n = _publish_one(con, settings, tdef)
            if n is not None:
                published[tdef.name] = n
    finally:
        con.close()
    log.info("published serving views", extra={"tables": published})
    return published


def serving_path(settings: Settings) -> Path:
    return settings.serving_db
