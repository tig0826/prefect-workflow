-- Google Maps Timeline Bronze Table DDL
-- Each row is one semanticSegment (visit or activity), flattened by sync_timeline.py
-- Files are JSONL (one JSON object per line) at s3a://bronze-zone/timeline/dt=YYYY-MM-DD/
CREATE TABLE IF NOT EXISTS hive.life_bronze.timeline_external (
    start_time VARCHAR,
    end_time VARCHAR,
    segment_type VARCHAR,
    place_id VARCHAR,
    place_semantic_type VARCHAR,
    place_lat_lng VARCHAR,
    activity_type VARCHAR,
    distance_meters DOUBLE,
    dt VARCHAR
) WITH (
    format = 'JSON',
    external_location = 's3a://bronze-zone/timeline',
    partitioned_by = ARRAY['dt']
);
