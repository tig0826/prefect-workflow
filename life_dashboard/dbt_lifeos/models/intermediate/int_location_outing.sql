{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH location_stays_filtered AS (
    SELECT *
    FROM {{ ref('location_stays_silver') }}
    WHERE ST_Distance(
        to_spherical_geography(ST_Point(139.72914345, 35.81176785)),
        to_spherical_geography(ST_Point(lng, lat))
    ) > 150.0
    {% if is_incremental() %}
      AND arrived_at >= CAST(date_add(
        'day',
        -{{ reprocess_days }},
        date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
      ) AS TIMESTAMP)
    {% endif %}
)

SELECT
    stay_pk AS event_pk,
    stay_date AS event_date_jst,
    'android_location' AS source_system,
    'location_stay' AS source_detail,
    CAST(arrived_at AS TIMESTAMP) AS start_ts,
    CAST(departed_at AS TIMESTAMP) AS end_ts,
    false AS is_afk,
    CAST(NULL AS VARCHAR) AS raw_app_name,
    CAST(NULL AS VARCHAR) AS raw_window_title,
    CAST(NULL AS VARCHAR) AS raw_usage_type,
    'OUTING' AS cat_main,
    '外出中' AS cat_sub,
    20 AS priority,
    CAST(NULL AS VARCHAR) AS hostname
FROM location_stays_filtered
