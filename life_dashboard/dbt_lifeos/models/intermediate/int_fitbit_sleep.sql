{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH src AS (
    SELECT *
    FROM {{ ref('fitbit_sleep') }}
    {% if is_incremental() %}
    WHERE start_time_jst >= date_add(
        'day',
        -{{ reprocess_days }},
        date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
    )
    {% endif %}
)

SELECT
    to_hex(md5(to_utf8('fitbit_sleep|' || CAST(log_id AS varchar)))) AS event_pk,
    CAST(start_time_jst AS date) AS event_date_jst,
    'fitbit' AS source_system,
    'fitbit_sleep' AS source_detail,
    CAST(start_time_jst AS TIMESTAMP) AS start_ts,
    CAST(end_time_jst AS TIMESTAMP) AS end_ts,
    CAST(NULL AS varchar) AS raw_app_name,
    CAST(NULL AS varchar) AS raw_window_title,
    CAST(NULL AS varchar) AS raw_usage_type,
    'SLEEP' AS cat_main,
    CASE WHEN is_main_sleep THEN '主睡眠' ELSE '昼寝' END AS cat_sub,
    100 AS priority
FROM src
