"""
Standalone geocoding flow: resolves uncached place_ids via Google Places API
and inserts them into life_gold.location_place_cache.

Independent of the ADB-based timeline export, so it keeps the cache fresh
even when the Android pipeline is down.
"""
import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from geocode_places import geocode_new_places

log = logging.getLogger(__name__)


@task(name="Geocode new place IDs")
def task_geocode():
    return geocode_new_places()


@flow(name="Google Places Geocoder", log_prints=True)
def geocode_flow():
    geocoded = task_geocode()
    print(f"Geocoded {geocoded} new places.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    geocode_flow()
