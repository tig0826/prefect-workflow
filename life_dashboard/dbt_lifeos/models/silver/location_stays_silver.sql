{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stay_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['stay_date']
) }}

WITH extracted AS (
    SELECT
        json_extract_scalar(raw_json, '$.date')      AS stay_date_str,
        json_extract_scalar(raw_json, '$.placeName') AS place_name,
        json_extract_scalar(raw_json, '$.placeId')   AS place_id,
        CAST(json_extract_scalar(raw_json, '$.lat')  AS DOUBLE) AS lat,
        CAST(json_extract_scalar(raw_json, '$.lng')  AS DOUBLE) AS lng,
        from_iso8601_timestamp(json_extract_scalar(raw_json, '$.arrivedAt'))  AS arrived_at,
        from_iso8601_timestamp(json_extract_scalar(raw_json, '$.departedAt')) AS departed_at,
        CAST(json_extract_scalar(raw_json, '$.durationMin') AS INTEGER) AS duration_min,
        dt AS source_dt
    FROM {{ source('hive_life_bronze', 'location_external') }}
    WHERE json_extract_scalar(raw_json, '$.type') = 'stay'
      AND raw_json IS NOT NULL
    {% if is_incremental() %}
      AND dt >= date_format(date_add('day', -3, current_date), '%Y-%m-%d')
    {% endif %}
)

SELECT
    to_hex(md5(to_utf8(
        COALESCE(place_id, place_name) || '|' || stay_date_str || '|' || CAST(arrived_at AS VARCHAR)
    ))) AS stay_pk,
    CAST(stay_date_str AS DATE) AS stay_date,
    place_name,
    place_id,
    lat,
    lng,
    arrived_at,
    departed_at,
    duration_min,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM extracted
WHERE arrived_at IS NOT NULL
  AND lat IS NOT NULL
