{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

SELECT
    aw_event_pk AS event_pk,
    hostname,
    afk_start_time_jst,
    afk_end_time_jst,
    afk_status,
    duration_sec
FROM {{ ref('aw_afk_events') }}
{% if is_incremental() %}
WHERE afk_start_time_jst >= date_add(
    'day',
    -{{ reprocess_days }},
    date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')
)
{% endif %}
