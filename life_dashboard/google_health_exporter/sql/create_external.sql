-- Fitbit の bronze は raw_json の中にさらに {"raw_json": ..., "dt": ...} が
-- 入る二重包みになっていて、silver 側が json_extract_scalar(raw_json, '$.raw_json')
-- で一段ほどいている。あれは事故の産物で読みにくいので、こちらは包まない。
-- raw_json 列にはペイロードが直接入る:
--   {"date": "...", "weight": [...], "body-fat": [...], "steps": [...], "sleep": [...]}
CREATE TABLE IF NOT EXISTS hive.life_bronze.google_health_external (
    raw_json VARCHAR,
    dt VARCHAR
) WITH (
    format = 'JSON',
    external_location = 's3a://bronze-zone/google_health/raw/',
    partitioned_by = ARRAY['dt']
);
