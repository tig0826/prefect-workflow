"""
Prefect flow: Google Maps Timeline パーティション同期

Android端末がMinIOに直接プッシュした後、
Hive外部テーブルのパーティションメタデータを更新する。
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow
from common.trino_tasks import create_external, sync_table_partition


@flow(name="Google Maps Timeline Partition Sync", log_prints=True)
def timeline_partition_sync_flow():
    create_external(system_name="timeline")
    sync_table_partition(table_name="timeline_external")
    print("✅ timeline_external パーティション同期完了")


if __name__ == "__main__":
    timeline_partition_sync_flow()
