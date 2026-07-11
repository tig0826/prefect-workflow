import sys
import os
import time
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from dbt_common.events.base_types import EventLevel

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.trino_tasks import sync_table_partition

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# A single dbt model erroring aborts the whole `build`, failing the flow and
# firing an alert. Most such errors are transient object-store blips: MinIO drives
# on the shared NFS/ZFS backend briefly go offline (30s drive health-check
# timeout) under IOPS pressure, so an Iceberg metadata/data read fails mid-build
# (ICEBERG_INVALID_METADATA / HIVE_FILESYSTEM_ERROR) even though the tables read
# fine seconds later. Retry the build after a short wait so these self-heal;
# genuine, persistent errors still surface once the attempts are exhausted.
# Root cause is infra (MinIO on a single NFS share) -- see the minio-on-nfs memo.
DBT_BUILD_ATTEMPTS = 3
DBT_RETRY_WAIT_SECONDS = 45


def _dbt_build_with_retry(select_args: list[str]) -> None:
    last_error: Exception | None = None
    for attempt in range(1, DBT_BUILD_ATTEMPTS + 1):
        try:
            PrefectDbtRunner(
                settings=PrefectDbtSettings(
                    project_dir=DBT_PROJECT_DIR,
                    profiles_dir=DBT_PROJECT_DIR,
                    log_level=EventLevel.INFO,
                )
            ).invoke(["build", *select_args])
            return
        except Exception as e:  # noqa: BLE001 - retry any build failure
            last_error = e
            if attempt < DBT_BUILD_ATTEMPTS:
                print(
                    f"dbt build {select_args} failed (attempt {attempt}/{DBT_BUILD_ATTEMPTS}): "
                    f"{e}. Likely a transient object-store blip (MinIO drive offline); "
                    f"retrying in {DBT_RETRY_WAIT_SECONDS}s."
                )
                time.sleep(DBT_RETRY_WAIT_SECONDS)
    assert last_error is not None
    raise last_error


@flow(name="LifeOS Integrated Data Pipeline")
def main_data_flow():
    sync_table_partition(table_name="timeline_external")
    print("STEP 1: Building Silver & Intermediate layers...")
    _dbt_build_with_retry(["--select", "models/silver", "models/intermediate"])

    print("STEP 2: Building Gold layer...")
    _dbt_build_with_retry(["--select", "models/gold", "--vars", "reprocess_days: 7"])


if __name__ == "__main__":
    main_data_flow()
