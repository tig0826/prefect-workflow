{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH hr_raw AS (
    SELECT timestamp_jst
    FROM {{ ref('fitbit_heartrate') }}
    WHERE timestamp_jst >= date_add(
        'day',
        -{{ reprocess_days + 1 }},
        date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
    )
),
hr_gaps_calc AS (
    SELECT
        timestamp_jst AS start_ts,
        COALESCE(LEAD(timestamp_jst) OVER (ORDER BY timestamp_jst), current_timestamp) AS end_ts
    FROM hr_raw
)
SELECT
    to_hex(md5(to_utf8('fitbit_hr_gap|' || CAST(start_ts AS varchar) || '|' || CAST(end_ts AS varchar)))) AS event_pk,
    CAST(start_ts AS date) AS event_date_jst,
    'fitbit' AS source_system,
    'fitbit_hr_gap' AS source_detail,
    CAST(start_ts AS TIMESTAMP) AS start_ts,
    CAST(end_ts AS TIMESTAMP) AS end_ts,
    CAST(NULL AS varchar) AS raw_app_name,
    CAST(NULL AS varchar) AS raw_window_title,
    CAST(NULL AS varchar) AS raw_usage_type,
    CASE WHEN date_diff('hour', start_ts, end_ts) >= 5 THEN 'UNKNOWN' ELSE 'BATH' END AS cat_main,
    CASE WHEN date_diff('hour', start_ts, end_ts) >= 5 THEN '不明' ELSE '入浴' END AS cat_sub,
    CASE WHEN date_diff('hour', start_ts, end_ts) >= 5 THEN 12 ELSE 15 END AS priority
FROM hr_gaps_calc
WHERE date_diff('minute', start_ts, end_ts) >= 15
  AND start_ts >= date_add(
      'day',
      -{{ reprocess_days }},
      date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
  )
