{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='segment_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw AS (
    SELECT
        start_time,
        end_time,
        segment_type,
        place_id,
        place_semantic_type,
        place_lat_lng,
        activity_type,
        distance_meters,
        ROW_NUMBER() OVER (
            PARTITION BY start_time, end_time, segment_type
            ORDER BY start_time
        ) AS rn
    FROM {{ source('hive_life_bronze', 'timeline_external') }}
    WHERE segment_type IN ('visit', 'activity')
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL
    {% if is_incremental() %}
      AND dt >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS VARCHAR)
    {% endif %}
),

src AS (
    SELECT * FROM raw WHERE rn = 1
)

SELECT
    to_hex(md5(to_utf8(start_time || '|' || end_time || '|' || segment_type))) AS segment_pk,
    CAST(
        CAST(from_iso8601_timestamp(start_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP)
        AS DATE
    ) AS event_date_jst,
    CAST(from_iso8601_timestamp(start_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS start_ts_jst,
    CAST(from_iso8601_timestamp(end_time) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS end_ts_jst,
    segment_type,
    place_id,
    place_semantic_type,
    place_lat_lng,
    activity_type,
    CAST(distance_meters AS DOUBLE) AS distance_meters,
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM src
