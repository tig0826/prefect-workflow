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
        CAST(json_extract(real_json, '$.sleep.sleep') AS ARRAY(JSON)) AS sleep_array
    FROM base
),

unnested AS (
    SELECT
        dt,
        CAST(json_extract_scalar(s.sleep_element, '$.logId') AS BIGINT) AS log_id,
        CAST(json_extract_scalar(s.sleep_element, '$.isMainSleep') AS BOOLEAN) AS is_main_sleep,
        -- 🌟 修正: ' Asia/Tokyo' を削除し、ただの TIMESTAMP としてパースする！
        CAST(REPLACE(json_extract_scalar(s.sleep_element, '$.startTime'), 'T', ' ') AS TIMESTAMP) AS start_time_jst,
        CAST(REPLACE(json_extract_scalar(s.sleep_element, '$.endTime'), 'T', ' ') AS TIMESTAMP) AS end_time_jst,
        CAST(json_extract_scalar(s.sleep_element, '$.duration') AS BIGINT) / 1000 AS duration_sec,
        CAST(json_extract_scalar(s.sleep_element, '$.minutesAsleep') AS INTEGER) AS minutes_asleep,
        CAST(json_extract_scalar(s.sleep_element, '$.efficiency') AS INTEGER) AS efficiency
    FROM extracted
    CROSS JOIN UNNEST(sleep_array) AS s(sleep_element)
)
SELECT * FROM unnested WHERE log_id IS NOT NULL
