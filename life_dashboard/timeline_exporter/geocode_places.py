"""
Geocode new place_id values from hive.life_bronze.timeline_external
using the Google Places API (New), and cache results in
iceberg.life_gold.location_place_cache.
"""
import contextlib
import logging
import time
from datetime import datetime, timezone, timedelta

import requests
import trino.dbapi

log = logging.getLogger(__name__)

TRINO_HOST = "trino.mynet"
TRINO_PORT = 80
TRINO_USER = "admin"

PLACES_API_BASE = "https://places.googleapis.com/v1/places"
SLEEP_BETWEEN_REQUESTS_S = 0.05  # 50 ms — stay well under rate limits


def _get_api_key() -> str:
    """Load the Google Places API key from Prefect Secret."""
    try:
        from prefect.blocks.system import Secret
        return Secret.load("google-places-api-key").get()
    except Exception:
        # Fallback for local testing outside Prefect
        import os
        return os.environ.get("GOOGLE_PLACES_API_KEY", "")


def _trino(catalog: str = "iceberg"):
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
    )


def _fetch_uncached_place_ids() -> list[str]:
    """Return place_ids in bronze that are not yet in the cache."""
    sql = """
        SELECT DISTINCT b.place_id
        FROM hive.life_bronze.timeline_external b
        WHERE b.place_id IS NOT NULL
          AND b.place_id NOT IN (
              SELECT place_id FROM iceberg.life_gold.location_place_cache
          )
    """
    with contextlib.closing(_trino(catalog="hive")) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def _lookup_place(place_id: str, api_key: str) -> dict | None:
    """Call Places API (New) for a single place_id. Returns dict or None."""
    if not api_key:
        log.warning("No API key — skipping geocoding")
        return None
    try:
        resp = requests.get(
            f"{PLACES_API_BASE}/{place_id}",
            params={"languageCode": "ja"},
            headers={
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "displayName,formattedAddress,location",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        loc = data.get("location", {})
        return {
            "place_id": place_id,
            "place_name": data.get("displayName", {}).get("text"),
            "formatted_address": data.get("formattedAddress"),
            "lat": loc.get("latitude"),
            "lng": loc.get("longitude"),
        }
    except Exception as e:
        log.error(f"Error geocoding {place_id}: {e}")
        return None


def _escape(v) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _insert_place(cursor, info: dict):
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S.%f")
    lat = str(info["lat"]) if info["lat"] is not None else "NULL"
    lng = str(info["lng"]) if info["lng"] is not None else "NULL"
    sql = f"""
        INSERT INTO iceberg.life_gold.location_place_cache
            (place_id, place_name, formatted_address, lat, lng, fetched_at)
        VALUES (
            {_escape(info['place_id'])},
            {_escape(info['place_name'])},
            {_escape(info['formatted_address'])},
            {lat},
            {lng},
            TIMESTAMP '{now_jst}' AT TIME ZONE 'Asia/Tokyo'
        )
    """
    cursor.execute(sql)
    cursor.fetchall()


def geocode_new_places() -> int:
    """Fetch names for all uncached place_ids. Returns count of newly cached places."""
    place_ids = _fetch_uncached_place_ids()
    if not place_ids:
        log.info("No new place_ids to geocode.")
        return 0

    log.info(f"Geocoding {len(place_ids)} new place_id(s).")
    api_key = _get_api_key()

    geocoded = 0
    with contextlib.closing(_trino(catalog="iceberg")) as conn:
        cur = conn.cursor()
        for place_id in place_ids:
            info = _lookup_place(place_id, api_key)
            if info:
                try:
                    _insert_place(cur, info)
                    geocoded += 1
                    log.info(f"  {place_id} → {info.get('place_name')}")
                except Exception as e:
                    log.error(f"  Insert failed for {place_id}: {e}")
            time.sleep(SLEEP_BETWEEN_REQUESTS_S)

    log.info(f"Done: {geocoded}/{len(place_ids)} places cached.")
    return geocoded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = geocode_new_places()
    print(f"Geocoded {count} new places.")
