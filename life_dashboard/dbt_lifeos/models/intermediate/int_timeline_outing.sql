{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH src AS (
    SELECT *
    FROM {{ ref('timeline_segments') }}
    WHERE (
        -- Visits away from home (TYPE_HOME is excluded)
        (segment_type = 'visit' AND (place_semantic_type IS NULL OR place_semantic_type != 'TYPE_HOME'))
        OR
        -- Vehicle or bicycle activity implies being away from home
        (segment_type = 'activity' AND activity_type IN ('IN_VEHICLE', 'ON_BICYCLE'))
    )
    {% if is_incremental() %}
      AND event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS DATE)
    {% endif %}
)

SELECT
    segment_pk AS event_pk,
    event_date_jst,
    'timeline' AS source_system,
    CASE
        WHEN segment_type = 'visit' THEN 'timeline_visit'
        ELSE 'timeline_activity'
    END AS source_detail,
    CAST(start_ts_jst AS TIMESTAMP) AS start_ts,
    CAST(end_ts_jst AS TIMESTAMP) AS end_ts,
    CAST(NULL AS VARCHAR) AS raw_app_name,
    CAST(NULL AS VARCHAR) AS raw_window_title,
    CAST(NULL AS VARCHAR) AS raw_usage_type,
    'OUTING' AS cat_main,
    CASE
        WHEN segment_type = 'visit' THEN '外出先滞在'
        WHEN activity_type = 'IN_VEHICLE' THEN '移動中(車)'
        WHEN activity_type = 'ON_BICYCLE' THEN '移動中(自転車)'
        ELSE '外出中'
    END AS cat_sub,
    20 AS priority
FROM src
