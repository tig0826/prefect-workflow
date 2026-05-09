{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='route_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['route_date']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

SELECT
    route_pk,
    route_date,
    transport_mode,
    started_at,
    ended_at,
    route_json
FROM {{ ref('location_routes_silver') }}
{% if is_incremental() %}
WHERE route_date >= date_add('day', -{{ reprocess_days }}, current_date)
{% endif %}
