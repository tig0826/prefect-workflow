{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='waypoint_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH src AS (
    SELECT start_time, end_time, route_points_json, dt
    FROM {{ source('hive_life_bronze', 'timeline_external') }}
    WHERE segment_type = 'timelinePath'
      AND route_points_json IS NOT NULL
    {% if is_incremental() %}
      AND dt >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS VARCHAR)
    {% endif %}
),

exploded AS (
    SELECT
        start_time,
        end_time,
        wp
    FROM src
    CROSS JOIN UNNEST(CAST(json_parse(route_points_json) AS ARRAY(JSON))) AS t(wp)
)

SELECT
    to_hex(md5(to_utf8(
        start_time || '|' || end_time || '|' ||
        COALESCE(json_extract_scalar(wp, '$.time'), '') || '|' ||
        COALESCE(json_extract_scalar(wp, '$.lat'), '') || '|' ||
        COALESCE(json_extract_scalar(wp, '$.lng'), '')
    ))) AS waypoint_pk,
    CAST(
        CAST(from_iso8601_timestamp(start_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS DATE
    ) AS event_date_jst,
    CAST(from_iso8601_timestamp(start_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS segment_start_jst,
    CAST(from_iso8601_timestamp(end_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS segment_end_jst,
    CAST(
        from_iso8601_timestamp(json_extract_scalar(wp, '$.time')) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP
    ) AS waypoint_time_jst,
    CAST(json_extract_scalar(wp, '$.lat') AS DOUBLE) AS lat,
    CAST(json_extract_scalar(wp, '$.lng') AS DOUBLE) AS lng,
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM exploded
WHERE json_extract_scalar(wp, '$.lat') IS NOT NULL
