{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH src AS (
    SELECT *
    FROM {{ ref('owntracks_stays') }} s
    WHERE ST_Distance(
        to_spherical_geography(ST_Point(139.72914345, 35.81176785)),
        to_spherical_geography(ST_Point(s.centroid_longitude, s.centroid_latitude))
    ) > 150.0
    {% if is_incremental() %}
      -- 🌟 修正1: 右辺を CAST(... AS TIMESTAMP) で包んで暗黙のキャストエラーを防ぐ
      AND s.stay_start_time >= CAST(date_add(
        'day',
        -{{ reprocess_days }},
        date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
      ) AS TIMESTAMP)
    {% endif %}
)

SELECT
    CAST(stay_pk AS varchar) AS event_pk,
    -- 🌟 修正2: すでにJSTなので AT TIME ZONE 'Asia/Tokyo' を剥ぎ取る！
    CAST(CAST(stay_start_time AS date) AS date) AS event_date_jst,
    'owntracks' AS source_system,
    'owntracks_outing' AS source_detail,
    CAST(stay_start_time AS TIMESTAMP) AS start_ts,
    CAST(stay_end_time AS TIMESTAMP) AS end_ts,
    CAST(NULL AS varchar) AS raw_app_name,
    CAST(NULL AS varchar) AS raw_window_title,
    CAST(NULL AS varchar) AS raw_usage_type,
    'OUTING' AS cat_main,
    '外出中' AS cat_sub,
    20 AS priority
FROM src
