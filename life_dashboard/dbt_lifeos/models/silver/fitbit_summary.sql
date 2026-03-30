{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dt',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        dt,
        -- 外側の殻を剥がして、本物のJSON文字列を取り出す
        json_extract_scalar(raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    {% if is_incremental() %}
    WHERE dt >= (SELECT MAX(dt) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        dt,
        -- サマリ系
        CAST(json_extract_scalar(real_json, '$.activities.summary.steps') AS INTEGER) AS steps,
        CAST(json_extract_scalar(real_json, '$.activities.summary.caloriesOut') AS INTEGER) AS calories_out,
        CAST(json_extract_scalar(real_json, '$.activities.summary.sedentaryMinutes') AS INTEGER) AS sedentary_minutes,
        -- 睡眠サマリ
        CAST(json_extract_scalar(real_json, '$.sleep.summary.totalMinutesAsleep') AS INTEGER) AS total_minutes_asleep,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.totalTimeInBed') AS INTEGER) AS total_time_in_bed,
        -- 心拍サマリ
        CAST(json_extract_scalar(real_json, '$.heart["activities-heart"][0].value.restingHeartRate') AS INTEGER) AS resting_heart_rate
    FROM base
)

SELECT * FROM parsed
