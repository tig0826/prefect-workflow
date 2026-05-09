{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stay_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['stay_date']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

SELECT
    stay_pk,
    stay_date,
    place_name,
    place_id,
    lat,
    lng,
    arrived_at,
    departed_at,
    duration_min
FROM {{ ref('location_stays_silver') }}
{% if is_incremental() %}
WHERE stay_date >= date_add('day', -{{ reprocess_days }}, current_date)
{% endif %}
