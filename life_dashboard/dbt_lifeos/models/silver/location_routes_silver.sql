{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='route_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['route_date']
) }}

WITH extracted AS (
    SELECT
        json_extract_scalar(raw_json, '$.date')  AS route_date_str,
        json_extract_scalar(raw_json, '$.mode')  AS transport_mode,
        from_iso8601_timestamp(json_extract_scalar(raw_json, '$.startedAt')) AS started_at,
        from_iso8601_timestamp(json_extract_scalar(raw_json, '$.endedAt'))   AS ended_at,
        json_format(json_extract(raw_json, '$.points')) AS route_json,
        dt AS source_dt
    FROM {{ source('hive_life_bronze', 'location_external') }}
    WHERE json_extract_scalar(raw_json, '$.type') = 'route'
      AND raw_json IS NOT NULL
    {% if is_incremental() %}
      AND dt >= date_format(date_add('day', -3, current_date), '%Y-%m-%d')
    {% endif %}
)

SELECT
    to_hex(md5(to_utf8(
        transport_mode || '|' || route_date_str || '|' || CAST(started_at AS VARCHAR)
    ))) AS route_pk,
    CAST(route_date_str AS DATE) AS route_date,
    transport_mode,
    started_at,
    ended_at,
    route_json,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM extracted
WHERE started_at IS NOT NULL
