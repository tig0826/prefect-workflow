CREATE TABLE IF NOT EXISTS hive.life_bronze.owntracks_external (
    raw_json VARCHAR,
    dt VARCHAR
) WITH (
    format = 'TEXTFILE',
    external_location = 's3a://bronze-zone/owntracks/compacted/',
    partitioned_by = ARRAY['dt']
)
