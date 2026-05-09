{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dt',
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
parsed AS (
    SELECT
        dt,
        -- 活動サマリ
        CAST(json_extract_scalar(real_json, '$.activities.summary.steps') AS INTEGER) AS steps,
        CAST(json_extract_scalar(real_json, '$.activities.summary.caloriesOut') AS INTEGER) AS calories_out,
        CAST(json_extract_scalar(real_json, '$.activities.summary.activityCalories') AS INTEGER) AS activity_calories,
        CAST(json_extract_scalar(real_json, '$.activities.summary.caloriesBMR') AS INTEGER) AS calories_bmr,
        CAST(json_extract_scalar(real_json, '$.activities.summary.floors') AS INTEGER) AS floors,
        CAST(json_extract_scalar(real_json, '$.activities.summary.elevation') AS DOUBLE) AS elevation,

        -- 活動の質
        CAST(json_extract_scalar(real_json, '$.activities.summary.sedentaryMinutes') AS INTEGER) AS sedentary_minutes,
        CAST(json_extract_scalar(real_json, '$.activities.summary.lightlyActiveMinutes') AS INTEGER) AS lightly_active_minutes,
        CAST(json_extract_scalar(real_json, '$.activities.summary.fairlyActiveMinutes') AS INTEGER) AS fairly_active_minutes,
        CAST(json_extract_scalar(real_json, '$.activities.summary.veryActiveMinutes') AS INTEGER) AS very_active_minutes,

        -- 睡眠サマリと質
        CAST(json_extract_scalar(real_json, '$.sleep.summary.totalMinutesAsleep') AS INTEGER) AS total_minutes_asleep,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.totalTimeInBed') AS INTEGER) AS total_time_in_bed,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.stages.deep') AS INTEGER) AS sleep_deep_minutes,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.stages.light') AS INTEGER) AS sleep_light_minutes,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.stages.rem') AS INTEGER) AS sleep_rem_minutes,
        CAST(json_extract_scalar(real_json, '$.sleep.summary.stages.wake') AS INTEGER) AS sleep_wake_minutes,

        -- 心拍
        CAST(json_extract_scalar(real_json, '$.heart["activities-heart"][0].value.restingHeartRate') AS INTEGER) AS resting_heart_rate,

        -- 体組成
        -- ※FitbitのAPIはデフォルトでlbs(ポンド)を返すため、kgに変換する
        -- 1 lb = 0.453592 kg
        ROUND(CAST(json_extract_scalar(real_json, '$.body.body.weight') AS DOUBLE) * 0.453592, 1) AS weight_kg,
        CAST(json_extract_scalar(real_json, '$.body.body.fat') AS DOUBLE) AS body_fat_pct,
        CAST(json_extract_scalar(real_json, '$.body.body.bmi') AS DOUBLE) AS bmi
    FROM base
)

SELECT * FROM parsed
