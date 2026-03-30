{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='log_id',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        dt,
        json_extract_scalar(raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    {% if is_incremental() %}
    WHERE dt >= (SELECT MAX(dt) FROM {{ this }})
    {% endif %}
),

extracted AS (
    SELECT
        dt,
        CAST(json_extract(real_json, '$.activities.activities') AS ARRAY(JSON)) AS activity_array
    FROM base
),

unnested AS (
    SELECT
        dt,
        CAST(json_extract_scalar(a.activity_element, '$.logId') AS BIGINT) AS log_id,
        json_extract_scalar(a.activity_element, '$.name') AS activity_name,
        CAST(json_extract_scalar(a.activity_element, '$.startDate') || ' ' || 
             json_extract_scalar(a.activity_element, '$.startTime') || ':00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE) AS start_time_jst,
        CAST(json_extract_scalar(a.activity_element, '$.duration') AS BIGINT) / 1000 AS duration_sec,
        CAST(json_extract_scalar(a.activity_element, '$.steps') AS INTEGER) AS steps,
        CAST(json_extract_scalar(a.activity_element, '$.calories') AS INTEGER) AS calories,
        CAST(json_extract_scalar(a.activity_element, '$.distance') AS DOUBLE) AS distance
    FROM extracted
    CROSS JOIN UNNEST(activity_array) AS a(activity_element)
)

SELECT * FROM unnested WHERE log_id IS NOT NULL
