-- ActivityWatch Bronze Table DDL Template (Universal)
CREATE TABLE IF NOT EXISTS hive.life_bronze.{aw_tablename} (
    id BIGINT,
    "timestamp" VARCHAR,
    duration DOUBLE,
    data MAP(VARCHAR, VARCHAR),
    dt VARCHAR
) WITH (
    format = 'JSON',
    external_location = 's3a://bronze-zone/aw/{aw_bucketname}',
    partitioned_by = ARRAY['dt']
);
