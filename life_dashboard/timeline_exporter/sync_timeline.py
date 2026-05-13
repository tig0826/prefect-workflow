"""
Diff-based Google Maps Timeline sync.

Reads the full タイムライン.json (85MB+), extracts segments newer than
the watermark, flattens each to a row, and uploads JSONL to MinIO Bronze.
"""
import json
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.client import Config

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.mynet")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "bronze-zone"
WATERMARK_KEY = "timeline/watermark.txt"


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def get_watermark(s3) -> str:
    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY)
        return obj["Body"].read().decode().strip()
    except Exception:
        return "2000-01-01T00:00:00.000+09:00"


def set_watermark(s3, ts: str):
    s3.put_object(Bucket=BRONZE_BUCKET, Key=WATERMARK_KEY, Body=ts.encode())


def _parse_latlng(latlng_str: str) -> tuple:
    """Parse '35.8118448°, 139.7291197°' into (lat_float, lng_float). Returns (None, None) on failure."""
    import re
    m = re.match(r"(-?[\d.]+)°,\s*(-?[\d.]+)°", latlng_str.strip())
    if not m:
        return None, None
    return float(m.group(1)), float(m.group(2))


def flatten_segment(s: dict) -> dict:
    """Flatten a semanticSegment object into a row suitable for Hive JSON external table."""
    row = {
        "start_time": s.get("startTime"),
        "end_time": s.get("endTime"),
        "segment_type": None,
        "place_id": None,
        "place_semantic_type": None,
        "place_lat_lng": None,
        "activity_type": None,
        "distance_meters": None,
        "route_points_json": None,
    }
    if "visit" in s:
        row["segment_type"] = "visit"
        tc = s["visit"].get("topCandidate", {})
        row["place_id"] = tc.get("placeId")
        row["place_semantic_type"] = tc.get("semanticType")
        loc = tc.get("placeLocation", {})
        if isinstance(loc, dict):
            row["place_lat_lng"] = loc.get("latLng")
    elif "activity" in s:
        row["segment_type"] = "activity"
        tc = s["activity"].get("topCandidate", {})
        row["activity_type"] = tc.get("type")
        row["distance_meters"] = s["activity"].get("distanceMeters")
    elif "timelinePath" in s:
        row["segment_type"] = "timelinePath"
        waypoints = []
        for pt in s.get("timelinePath", []):
            point_str = pt.get("point", "")
            time_str = pt.get("time", "")
            lat, lng = _parse_latlng(point_str)
            if lat is not None and lng is not None:
                waypoints.append({"lat": lat, "lng": lng, "time": time_str})
        row["route_points_json"] = json.dumps(waypoints, ensure_ascii=False)
    return row


def sync(timeline_path: str) -> int:
    s3 = get_s3()
    watermark = get_watermark(s3)
    log.info(f"Watermark: {watermark}")

    with open(timeline_path, encoding="utf-8") as f:
        data = json.load(f)

    all_segments = data.get("semanticSegments", [])
    new_segments = [
        s for s in all_segments
        if s.get("startTime", "") > watermark
        and s.get("startTime") is not None
        and s.get("endTime") is not None
    ]

    if not new_segments:
        log.info("No new segments since watermark.")
        return 0

    log.info(f"New segments: {len(new_segments)} (of {len(all_segments)} total)")

    # startTime の日付でパーティション分割してアップロード
    from collections import defaultdict
    groups: dict[str, list] = defaultdict(list)
    for s in new_segments:
        date = s["startTime"][:10]  # YYYY-MM-DD
        groups[date].append(s)

    ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    total_rows = 0
    for date in sorted(groups.keys()):
        segs = groups[date]
        key = f"timeline/dt={date}/{ts_str}.json"
        rows = [flatten_segment(s) for s in segs]
        payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        log.info(f"  dt={date}: {len(rows)} rows -> s3://{BRONZE_BUCKET}/{key}")
        total_rows += len(rows)

    new_watermark = max(s["endTime"] for s in new_segments)
    set_watermark(s3, new_watermark)
    log.info(f"Watermark updated -> {new_watermark} (total {total_rows} rows uploaded)")

    return len(new_segments)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/timeline.json"
    count = sync(path)
    print(f"Synced {count} new segments.")
