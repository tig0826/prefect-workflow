"""Iceberg table maintenance for the dqx schema.

Runs daily to keep the price tables healthy on the (NFS-backed) MinIO store:
for each table we optimize (merge small files), expire old snapshots, then
remove the now-orphaned files. The last step is what actually caps the object
count that drives MinIO/NFS list latency.

Kept deliberately parallel to life_dashboard/common/compaction_flow.py so both
projects maintain their Iceberg tables with the same optimize -> expire ->
remove-orphans sequence and the same retention policy.
"""
from prefect import flow, task, get_run_logger
from trino.dbapi import connect

TRINO_HOST = "trino.trino.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "tig"
CATALOG = "iceberg"
SCHEMA = "dqx"

# Maintenance policy (shared vocabulary with the life_dashboard flow).
FILE_SIZE_THRESHOLD = "128MB"  # optimize rewrites only files smaller than this
RETENTION = "7d"               # snapshots / orphan files older than this are reclaimed

MART_TABLES = [
    "mrt_price_hourly",
    "mrt_price_daily",
    "mrt_price_short_baseline",
]


def _run_ddl(sql: str):
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=CATALOG)
    cur = conn.cursor()
    cur.execute(sql)
    cur.fetchall()
    conn.close()


def _table(table: str) -> str:
    return f'{CATALOG}.{SCHEMA}."{table}"'


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def optimize_table(table: str):
    """Compact small files across the whole table."""
    get_run_logger().info(f"Optimizing {table}...")
    _run_ddl(f"ALTER TABLE {_table(table)} EXECUTE optimize(file_size_threshold => '{FILE_SIZE_THRESHOLD}')")


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def optimize_price_hourly():
    """Compact only the last 7 days of price_hourly.

    Historical partitions are immutable, and optimizing the full table at once
    can overwhelm the Trino workers, so restrict to the recently-written range.
    """
    get_run_logger().info("Optimizing price_hourly (last 7 days)...")
    _run_ddl(
        f"ALTER TABLE {_table('price_hourly')} EXECUTE optimize(file_size_threshold => '{FILE_SIZE_THRESHOLD}')"
        " WHERE observed_at >= current_date - INTERVAL '7' DAY"
    )


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def expire_snapshots(table: str):
    """Drop snapshots older than RETENTION so their data files stop being referenced."""
    get_run_logger().info(f"Expiring snapshots for {table} (>{RETENTION})...")
    _run_ddl(f"ALTER TABLE {_table(table)} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')")


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def remove_orphan_files(table: str):
    """Delete files on the object store no longer referenced by any snapshot.

    expire_snapshots only unlinks them from metadata; this is what reclaims the
    space and caps the object count.
    """
    get_run_logger().info(f"Removing orphan files for {table} (>{RETENTION})...")
    _run_ddl(f"ALTER TABLE {_table(table)} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')")


def _maintain(table: str):
    expire_snapshots(table)
    remove_orphan_files(table)


@flow(log_prints=True)
def iceberg_maintenance():
    # price_hourly is large: optimize is partition-scoped to the recent range.
    optimize_price_hourly()
    _maintain("price_hourly")
    # The marts are small enough to optimize in full.
    for table in MART_TABLES:
        optimize_table(table)
        _maintain(table)
