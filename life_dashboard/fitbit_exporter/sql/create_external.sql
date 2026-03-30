CREATE TABLE IF NOT EXISTS hive.life_bronze.fitbit_external (
    raw_json VARCHAR,
    dt VARCHAR
) WITH (
    format = 'JSON',
    external_location = 's3a://bronze-zone/fitbit/raw/',
    partitioned_by = ARRAY['dt']
);
