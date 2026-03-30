{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='heartrate_pk',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        dt,
        json_extract_scalar(raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    {% if is_incremental() %}
    WHERE dt >= (SELECT MAX(SUBSTR(CAST(timestamp_jst AS VARCHAR), 1, 10)) FROM {{ this }})
    {% endif %}
),

extracted AS (
    SELECT
        dt,
        CAST(json_extract(real_json, '$.heart["activities-heart-intraday"].dataset') AS ARRAY(JSON)) AS hr_array
    FROM base
),

unnested AS (
    SELECT
        dt,
        json_extract_scalar(h.hr_element, '$.time') AS time_str,
        CAST(json_extract_scalar(h.hr_element, '$.value') AS INTEGER) AS heart_rate
    FROM extracted
    CROSS JOIN UNNEST(hr_array) AS h(hr_element)
),

final_transformed AS (
    SELECT
        to_hex(md5(to_utf8(dt || 'T' || time_str))) AS heartrate_pk,
        CAST(dt || ' ' || time_str || ' Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE) AS timestamp_jst,
        heart_rate
    FROM unnested
    WHERE time_str IS NOT NULL
)

SELECT * FROM final_transformed
