"""
Iceberg table compaction flow.

Runs weekly to prevent small-file accumulation in incremental merge tables.
"""
import logging
from prefect import flow, task
from common.trino_api import TrinoAPI

log = logging.getLogger(__name__)

TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

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
    # Intermediate
    ("life_gold", "int_aw_afk"),
    ("life_gold", "int_aw_categorized"),
    ("life_gold", "int_fitbit_activity"),
    ("life_gold", "int_fitbit_hr_gaps"),
    ("life_gold", "int_fitbit_sleep"),
    ("life_gold", "int_owntracks_outing"),
    # Gold marts
    ("life_gold", "mrt_aw_daily_work_summary"),
    ("life_gold", "mrt_behavior_slots_15m"),
    ("life_gold", "mrt_fitness_daily_summary"),
]


@task(name="Optimize Iceberg table", retries=1, retry_delay_seconds=10)
def optimize_table(schema: str, table: str):
    sql = f"ALTER TABLE {schema}.{table} EXECUTE optimize"
    TRINO.execute_action(sql)
    log.info(f"Optimized: {schema}.{table}")


@flow(name="Iceberg Compaction", log_prints=True)
def iceberg_compaction_flow():
    """Compact all Iceberg tables to merge small files. Runs weekly."""
    print("=== Compacting all Iceberg tables ===")
    for schema, table in ICEBERG_TABLES:
        optimize_table(schema, table)
    print("Compaction complete.")


if __name__ == "__main__":
    iceberg_compaction_flow()
