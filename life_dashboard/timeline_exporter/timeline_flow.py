"""
Prefect flow: Google Maps Timeline export & sync.
Runs every 6 hours via cron.
"""
import os
import sys
import tempfile
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from prefect.variables import Variable
from export_timeline import import_from_backup, export_timeline, pull_timeline
from sync_timeline import sync
from common.trino_tasks import create_external, sync_table_partition

log = logging.getLogger(__name__)


@task(name="Import Timeline backup via ADB", retries=1, retry_delay_seconds=30)
def task_import():
    os.environ["ADB_DEVICE"] = Variable.get("timeline-adb-device")
    import_from_backup()


@task(name="Export Timeline via ADB", retries=1, retry_delay_seconds=30)
def task_export():
    os.environ["ADB_DEVICE"] = Variable.get("timeline-adb-device")
    export_timeline()


@task(name="Pull Timeline JSON", retries=2, retry_delay_seconds=10)
def task_pull(local_path: str):
    os.environ["ADB_DEVICE"] = Variable.get("timeline-adb-device")
    pull_timeline(local_path)


@task(name="Sync Timeline diff to MinIO")
def task_sync(local_path: str) -> int:
    return sync(local_path)


@task(name="Geocode new place IDs")
def task_geocode():
    from geocode_places import geocode_new_places
    return geocode_new_places()


@flow(name="Google Maps Timeline Sync", log_prints=True)
def timeline_sync_flow():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        local_path = f.name

    task_import()
    task_export()
    task_pull(local_path)
    count = task_sync(local_path)
    print(f"Done. {count} new segments synced.")

    if count > 0:
        create_external(system_name="timeline")
        sync_table_partition(table_name="timeline_external")

    # Always retry uncached place_ids so transient API failures get recovered
    # on the next run, even when no new segments arrived this time.
    geocoded = task_geocode()
    print(f"Geocoded {geocoded} new places.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    timeline_sync_flow()
