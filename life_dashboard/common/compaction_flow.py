"""
Iceberg table compaction flow.

Runs daily to prevent small-file accumulation in incremental merge tables.

Targets are discovered mechanically from the dbt-managed lifeos schemas
(life_silver / life_gold) rather than a hardcoded list, so newly added dbt
models are picked up automatically. The raw ingestion layers (life / life_bronze)
are intentionally excluded: they hold non-Iceberg external tables that don't
support EXECUTE optimize.
"""
import logging
from prefect import flow, task
from common.trino_api import TrinoAPI

log = logging.getLogger(__name__)

TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

# dbt writes managed Iceberg tables here (see dbt_lifeos/dbt_project.yml:
# silver -> life_silver, intermediate/gold -> life_gold).
TARGET_SCHEMAS = ["life_silver", "life_gold"]


@task(name="Discover Iceberg tables")
def discover_tables() -> list[tuple[str, str]]:
    """List base tables in the target schemas, skipping dbt transient temp tables.

    Views (dbt models materialized as view) are excluded via table_type: they
    don't support ALTER TABLE EXECUTE optimize.
    """
    schema_list = ", ".join(f"'{s}'" for s in TARGET_SCHEMAS)
    df = TRINO.execute_query(
        "SELECT table_schema, table_name "
        "FROM iceberg.information_schema.tables "
        f"WHERE table_schema IN ({schema_list}) "
        "AND table_type = 'BASE TABLE' "
        "AND table_name NOT LIKE '%__dbt_tmp' "
        "ORDER BY table_schema, table_name"
    )
    tables = [(row.table_schema, row.table_name) for row in df.itertuples()]
    log.info(f"Discovered {len(tables)} table(s) across {TARGET_SCHEMAS}")
    return tables


# Maintenance policy (shared vocabulary with dqx/iceberg_maintenance).
FILE_SIZE_THRESHOLD = "128MB"  # optimize rewrites only files smaller than this
RETENTION = "7d"               # snapshots / orphan files older than this are reclaimed

# Raised when a maintenance procedure is run against a non-Iceberg external
# table or a view. Those don't support it, so skip rather than fail the flow.
_SKIP_MARKERS = ("Not an Iceberg table", "is not supported for views")


def _execute(action: str, sql: str, schema: str, table: str) -> bool:
    """Run a maintenance DDL; skip relations that don't support it.

    Returns True if it ran, False if skipped. Real failures propagate (with the
    task's retries) so a genuine storage error still surfaces.
    """
    try:
        TRINO.execute_action(sql)
        log.info(f"{action}: {schema}.{table}")
        return True
    except Exception as e:
        if any(m in str(e) for m in _SKIP_MARKERS):
            log.warning(f"Skipped {action} (unsupported relation): {schema}.{table}")
            return False
        raise


@task(name="Maintain Iceberg table", retries=2, retry_delay_seconds=30)
def maintain_table(schema: str, table: str):
    """optimize -> expire_snapshots -> remove_orphan_files for one table.

    Order matters: optimize creates a new snapshot over the compacted files,
    expire_snapshots then unlinks the pre-compaction small files, and
    remove_orphan_files finally deletes them from the object store. Optimize
    alone would grow the object count unbounded -- the in-repo contributor to
    the MinIO/NFS list-latency slowdown.
    """
    rel = f'"{schema}"."{table}"'
    if not _execute(
        "Optimized",
        f"ALTER TABLE {rel} EXECUTE optimize(file_size_threshold => '{FILE_SIZE_THRESHOLD}')",
        schema, table,
    ):
        return  # non-Iceberg / view: expire & remove won't apply either
    _execute(
        "Expired snapshots",
        f"ALTER TABLE {rel} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')",
        schema, table,
    )
    _execute(
        "Removed orphan files",
        f"ALTER TABLE {rel} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')",
        schema, table,
    )


@flow(name="Iceberg Compaction", log_prints=True)
def iceberg_compaction_flow():
    """Compact + reclaim all lifeos Iceberg tables to cap object count. Runs daily."""
    print("=== Maintaining all lifeos Iceberg tables ===")
    tables = discover_tables()
    for schema, table in tables:
        maintain_table(schema, table)
    print(f"Maintenance complete ({len(tables)} tables).")


if __name__ == "__main__":
    iceberg_compaction_flow()
