{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH src AS (
    SELECT *
    FROM {{ ref('fitbit_activity') }}
    WHERE LOWER(activity_name) NOT IN ('walk', 'walking')
    {% if is_incremental() %}
      AND start_time_jst >= date_add(
        'day',
        -{{ reprocess_days }},
        date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
      )
    {% endif %}
)

SELECT
    to_hex(md5(to_utf8('fitbit_activity|' || CAST(log_id AS varchar)))) AS event_pk,
    CAST(start_time_jst AS date) AS event_date_jst,
    'fitbit' AS source_system,
    'fitbit_activity' AS source_detail,
    CAST(start_time_jst AS TIMESTAMP) AS start_ts,
    CAST(start_time_jst + (duration_sec * INTERVAL '1' SECOND) AS TIMESTAMP) AS end_ts,
    CAST(activity_name AS varchar) AS raw_app_name,
    CAST(NULL AS varchar) AS raw_window_title,
    CAST(NULL AS varchar) AS raw_usage_type,
    'HEALTH' AS cat_main,
    activity_name AS cat_sub,
    90 AS priority
FROM src
