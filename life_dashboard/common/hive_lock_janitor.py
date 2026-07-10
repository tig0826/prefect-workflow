"""
Hive Metastore lock janitor.

Trino's Iceberg connector takes exclusive locks in the Hive Metastore
(the `hive_locks` table in the `iceberg_catalog` Postgres DB on CNPG core-db) for
table commits. When a dbt run is killed or times out -- e.g. the life_data_pipeline
CANCEL_NEW policy cancelling an overlapping run -- its lock cleanup never runs and
the exclusive lock is orphaned. A live session heartbeats its lock every few
minutes; an orphaned one stops heartbeating. The stale exclusive lock then blocks
every subsequent write to that table:

    HIVE_TABLE_LOCK_NOT_ACQUIRED: Timed out waiting for lock NNNNN

which floods the pipeline with FAILED runs (fitbit_activity, aw_*, asken_*, ...)
until the row is cleared by hand. This flow does that clearing on a schedule.

It deletes lock rows whose heartbeat is older than the staleness threshold
(default 30 min) -- far beyond any legitimate heartbeat gap (~a few minutes), so
it only ever removes dead locks and never a live session's lock. Only rows with
hl_txnid = 0 are touched: those are the Iceberg/Trino table locks, not Hive ACID
transaction locks.

Connection notes:
- Connects via the CNPG *read-write service* `core-db-rw`, never a pod name:
  CNPG renumbers pods (core-db-3 -> core-db-4 -> ...) on failover, but the
  service always points at the current primary (writes require the primary).
- Authenticates as `tig`, the app user Trino's metastore itself uses and the
  owner of hive_locks. Password comes from the Prefect Secret block
  `core-db-tig-passwd` (a snapshot of the CNPG `core-db-app-user` secret, which
  is stable across failovers).
"""
import asyncio

from prefect import flow
from prefect.blocks.system import Secret

# CNPG read-write service -> current primary. Do NOT hardcode a pod (core-db-N):
# the numeric suffix changes on failover.
CORE_DB_HOST = "core-db-rw.cnpg.svc.cluster.local"
CORE_DB_PORT = 5432
CATALOG_DB = "iceberg_catalog"
DB_USER = "tig"
DB_PASSWORD_SECRET = "core-db-tig-passwd"

# A live Hive lock heartbeats every few minutes; 30 min without a heartbeat means
# the owning session is gone. Well above any legitimate gap, so this only clears
# genuinely dead locks.
STALE_MINUTES = 30

# hl_last_heartbeat is epoch milliseconds; compare against now() the same way.
_STALE_MS_EXPR = "(extract(epoch from now()) * 1000 - hl_last_heartbeat)"


async def _clear_stale_locks(password: str, stale_minutes: int) -> list[dict]:
    """Delete stale orphaned hive_locks rows; return the rows that were removed."""
    import asyncpg

    threshold_ms = stale_minutes * 60 * 1000
    conn = await asyncpg.connect(
        host=CORE_DB_HOST,
        port=CORE_DB_PORT,
        database=CATALOG_DB,
        user=DB_USER,
        password=password,
        ssl="prefer",
        timeout=15,
    )
    try:
        # Delete and return the affected rows in one statement so the reported
        # set exactly matches what was removed (no read/delete race).
        rows = await conn.fetch(
            f"""
            DELETE FROM hive_locks
            WHERE hl_txnid = 0
              AND {_STALE_MS_EXPR} > $1
            RETURNING hl_lock_ext_id, hl_db, hl_table, hl_lock_state,
                      round({_STALE_MS_EXPR} / 60000.0, 1) AS stale_min
            """,
            threshold_ms,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@flow(name="Hive Lock Janitor", log_prints=True)
def hive_lock_janitor(stale_minutes: int = STALE_MINUTES):
    """Clear orphaned Hive Metastore locks that block Iceberg table writes.

    Runs frequently (see the deployment schedule) so a stuck lock is cleared
    within one interval instead of flooding the pipeline with lock-timeout
    failures until manual intervention.
    """
    password = Secret.load(DB_PASSWORD_SECRET).get()
    cleared = asyncio.run(_clear_stale_locks(password, stale_minutes))

    if not cleared:
        print(f"No stale locks (heartbeat older than {stale_minutes} min). Nothing to clear.")
        return 0

    for r in cleared:
        print(
            f"cleared orphaned lock: id={r['hl_lock_ext_id']} "
            f"{r['hl_db']}.{r['hl_table']} state={r['hl_lock_state']} "
            f"stale_min={r['stale_min']}"
        )
    print(f"Cleared {len(cleared)} stale orphaned lock(s).")
    return len(cleared)


if __name__ == "__main__":
    hive_lock_janitor()
