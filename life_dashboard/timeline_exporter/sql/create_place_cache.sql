CREATE TABLE IF NOT EXISTS iceberg.life_gold.location_place_cache (
    place_id VARCHAR,
    place_name VARCHAR,
    formatted_address VARCHAR,
    lat DOUBLE,
    lng DOUBLE,
    fetched_at TIMESTAMP(6) WITH TIME ZONE
) WITH (
    format = 'parquet'
);
