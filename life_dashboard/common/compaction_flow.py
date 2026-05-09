"""
Iceberg table compaction flow.

Runs weekly to prevent small-file accumulation in incremental merge tables.
Also applies partition evolution to tables that lack partitioning.
"""
import logging
from prefect import flow, task
from common.trino_api import TrinoAPI

log = logging.getLogger(__name__)

TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

# All Iceberg tables to optimize (schema, table)
ICEBERG_TABLES = [
    # Silver
    ("life_silver", "aw_window_events"),
    ("life_silver", "aw_afk_events"),
    ("life_silver", "asken_meal"),
    ("life_silver", "asken_nutrition"),
    ("life_silver", "fitbit_activity"),
    ("life_silver", "fitbit_heartrate"),
    ("life_silver", "fitbit_sleep"),
    ("life_silver", "fitbit_summary"),
    ("life_silver", "owntracks_locations"),
    ("life_silver", "owntracks_stays"),
    # Intermediate (all in life_gold schema)
    ("life_gold", "int_aw_afk"),
    ("life_gold", "int_aw_categorized"),
    ("life_gold", "int_fitbit_activity"),
    ("life_gold", "int_fitbit_hr_gaps"),
    ("life_gold", "int_fitbit_sleep"),
    ("life_gold", "int_location_outing"),
    ("life_gold", "int_owntracks_outing"),
    # Gold marts
    ("life_gold", "mrt_aw_daily_work_summary"),
    ("life_gold", "mrt_behavior_slots_15m"),
    ("life_gold", "mrt_fitness_daily_summary"),
    ("life_gold", "location_routes"),
    ("life_gold", "location_stays"),
]

# Partition fields to add via Iceberg partition evolution (schema, table, field_expr)
# These align with the partitioned_by configs in dbt models.
# Safe to re-run: errors for already-existing partition fields are caught and ignored.
PARTITION_FIELDS = [
    ("life_gold", "int_aw_afk",               "event_date_jst"),
    ("life_gold", "int_aw_categorized",        "event_date_jst"),
    ("life_gold", "int_fitbit_activity",       "event_date_jst"),
    ("life_gold", "int_fitbit_hr_gaps",        "event_date_jst"),
    ("life_gold", "int_fitbit_sleep",          "event_date_jst"),
    ("life_gold", "int_location_outing",       "event_date_jst"),
    ("life_gold", "int_owntracks_outing",      "event_date_jst"),
    ("life_gold", "mrt_aw_daily_work_summary", "target_date"),
    ("life_gold", "mrt_behavior_slots_15m",    "slot_date_jst"),
    ("life_gold", "mrt_fitness_daily_summary", "target_date"),
    ("life_silver", "fitbit_heartrate",        "day(timestamp_jst)"),
]


@task(name="Add Iceberg partition field", retries=1, retry_delay_seconds=5)
def add_partition_field(schema: str, table: str, field_expr: str):
    sql = f"ALTER TABLE {schema}.{table} ADD PARTITION FIELD {field_expr}"
    try:
        TRINO.execute_action(sql)
        log.info(f"Partition field added: {schema}.{table} [{field_expr}]")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "duplicate" in msg or "partition field" in msg:
            log.info(f"Partition field already exists, skipping: {schema}.{table} [{field_expr}]")
        else:
            raise


@task(name="Optimize Iceberg table", retries=1, retry_delay_seconds=10)
def optimize_table(schema: str, table: str):
    sql = f"ALTER TABLE {schema}.{table} EXECUTE optimize"
    TRINO.execute_action(sql)
    log.info(f"Optimized: {schema}.{table}")


@flow(name="Iceberg Compaction", log_prints=True)
def iceberg_compaction_flow():
    """
    1. Apply partition evolution to tables that need it (idempotent).
    2. Compact all Iceberg tables to merge small files.
    Run this after partition fields are added so OPTIMIZE rewrites files
    under the new partition scheme.
    """
    print("=== Step 1: Applying partition evolution ===")
    for schema, table, field_expr in PARTITION_FIELDS:
        add_partition_field(schema, table, field_expr)

    print("=== Step 2: Compacting all Iceberg tables ===")
    for schema, table in ICEBERG_TABLES:
        optimize_table(schema, table)

    print("✅ Compaction complete.")


if __name__ == "__main__":
    iceberg_compaction_flow()
