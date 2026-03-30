import io
import gzip
import json
import logging
import datetime as dt
from typing import Iterable, Dict, Any, List, Optional

from prefect import flow, task

from common.storage_tasks import connect_s3_client, save_bytes_to_s3
from common.trino_tasks import create_external, sync_table_partition

logger = logging.getLogger(__name__)


def iter_events_from_gz(gz_bytes: bytes) -> Iterable[Dict[str, Any]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as f:
        obj = json.loads(f.read().decode("utf-8"))
    if isinstance(obj, list):
        for ev in obj:
            if isinstance(ev, dict):
                yield ev
    elif isinstance(obj, dict):
        yield obj


@task(retries=3, retry_delay_seconds=10, name="List OwnTracks Raw Keys")
def list_raw_keys(date_str: str) -> List[str]:
    s3, bucket = connect_s3_client()
    prefix = f"owntracks/raw/date={date_str}/"
    keys: List[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys += [
            o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".log.gz")
        ]
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    logger.info(f"Found {len(keys)} raw objects for date {date_str}")
    return keys


@task(retries=3, retry_delay_seconds=10, name="Compact JSON to Gzip Bytes")
def compact_events_to_bytes(keys: List[str]) -> Optional[bytes]:
    """S3から読み込んだ細切れJSONをGzipバイナリに変換して返す"""
    if not keys:
        return None
    s3, bucket = connect_s3_client()
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as gz:
        event_count = 0
        for key in keys:
            resp = s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read()
            for ev in iter_events_from_gz(body):
                line = json.dumps(ev, ensure_ascii=False) + "\n"
                gz.write(line.encode("utf-8"))
                event_count += 1
    logger.info(f"Compacted {event_count} events into memory.")
    return out.getvalue()


@flow(name="OwnTracks Hourly flow", log_prints=True)
def owntracks_flow(date_str: Optional[str] = None):
    """
    毎時実行し、ログをまるごと1つのファイルに結合するFlow。
    日付変更時のデータ漏れを防ぐため、常に「昨日」と「今日」の2日分を処理する。
    """
    # vectorで収集したデータに対してコンパクションを実施
    if date_str:
        # 手動で日付指定された場合はその日だけを処理
        target_dates = [date_str]
    else:
        # スケジュール実行時は、常に「昨日」と「今日」を対象にする
        jst = dt.timezone(dt.timedelta(hours=9))
        now = dt.datetime.now(jst)
        target_dates = [
            (now - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        ]
    for d in target_dates:
        logger.info(f"Starting compaction flow for date: {d}")
        keys = list_raw_keys(d)
        gzipped_bytes = compact_events_to_bytes(keys)
        if gzipped_bytes:
            save_bytes_to_s3(
                data=gzipped_bytes,
                prefix=f"owntracks/compacted/dt={d}",
                file_name="data.jsonl.gz",
                content_type="application/gzip",
            )
            logger.info(f"Compaction flow finished successfully for {d}.")
        else:
            logger.info(f"No data to process for {d}. Flow finished.")
    # external tableを作成
    logger.info("Syncing Trino external table and partitions...")
    create_external(system_name="owntracks")
    sync_table_partition(table_name="owntracks_external")
    logger.info("✅ All tasks completed successfully.")


if __name__ == "__main__":
    owntracks_flow()
