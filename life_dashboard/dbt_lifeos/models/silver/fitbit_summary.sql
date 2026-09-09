{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dt',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH base AS (
    SELECT
        dt,
        json_extract_scalar(raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    {% if is_incremental() %}
    -- dt >= MAX(dt) だけだと「翌日の dt が現れた瞬間に前日を二度と読み直さない」
    -- ため、その一瞬 bronze が欠けていた日（Fitbit API の一時失敗や
    -- トークン切れ）が恒久的に凍結する。bronze が後から復旧しても
    -- silver は NULL のままで、gold の COALESCE により 0歩・0kcal・睡眠なしと
    -- して表示され続ける。直近 N 日を毎回読み直して自己修復させる。
    -- 1日1行のモデルなので再マージのコストは無視できる。
    WHERE dt >= (
        SELECT CAST(date_add('day', -{{ reprocess_days }}, CAST(MAX(dt) AS DATE)) AS VARCHAR)
        FROM {{ this }}
    )
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

        -- 体組成（レガシー Fitbit 由来。移行後は下の Google Health を優先し、
        -- こちらは Google Health の bronze がまだ無い過去日のための保険）
        -- ※FitbitのAPIはデフォルトでlbs(ポンド)を返すため、kgに変換する
        -- 1 lb = 0.453592 kg
        ROUND(CAST(json_extract_scalar(real_json, '$.body.body.weight') AS DOUBLE) * 0.453592, 1) AS legacy_weight_kg,
        CAST(json_extract_scalar(real_json, '$.body.body.fat') AS DOUBLE) AS legacy_body_fat_pct,
        CAST(json_extract_scalar(real_json, '$.body.body.bmi') AS DOUBLE) AS legacy_bmi
    FROM base
),

-- ここから体重・体脂肪の取得元を Google Health に移す。
-- レガシー Fitbit Web API は体重の配信を 2026-08-31 で止めており、
-- body/date は {"weight":0} を返すだけになった（アプリには値がある）。
--
-- 同じ実測が最大3系統（GOOGLE_WEB_API / HEALTH_CONNECT / FITBIT_WEB_API）
-- から重複して入るので、(時刻, 値) で潰してから代表値を出す。
gh_raw AS (
    SELECT
        g.dt,
        p
    FROM {{ source('hive_life_bronze', 'google_health_external') }} g
    CROSS JOIN UNNEST(
        CAST(json_extract(json_parse(g.raw_json), '$.weight') AS ARRAY(JSON))
    ) AS t(p)
    {% if is_incremental() %}
    WHERE g.dt >= (
        SELECT CAST(date_add('day', -{{ reprocess_days }}, CAST(MAX(dt) AS DATE)) AS VARCHAR)
        FROM {{ this }}
    )
    {% endif %}
),
gh_weight AS (
    SELECT DISTINCT
        dt,
        json_extract_scalar(p, '$.weight.sampleTime.physicalTime') AS ts,
        CAST(json_extract_scalar(p, '$.weight.weightGrams') AS DOUBLE) AS grams
    FROM gh_raw
),
gh_fat_raw AS (
    SELECT
        g.dt,
        p
    FROM {{ source('hive_life_bronze', 'google_health_external') }} g
    CROSS JOIN UNNEST(
        -- キーにハイフンが入るので $."body-fat" ではなくブラケット記法。
        -- 前者は Trino が Invalid JSON path で拒否する。
        CAST(json_extract(json_parse(g.raw_json), '$["body-fat"]') AS ARRAY(JSON))
    ) AS t(p)
    {% if is_incremental() %}
    WHERE g.dt >= (
        SELECT CAST(date_add('day', -{{ reprocess_days }}, CAST(MAX(dt) AS DATE)) AS VARCHAR)
        FROM {{ this }}
    )
    {% endif %}
),
gh_fat AS (
    SELECT DISTINCT
        dt,
        json_extract_scalar(p, '$.bodyFat.sampleTime.physicalTime') AS ts,
        CAST(json_extract_scalar(p, '$.bodyFat.percentage') AS DOUBLE) AS pct
    FROM gh_fat_raw
),

-- 1日に最大10回測っていて日内で1kg以上ばらつくため、代表値は中央値を採る。
-- 食事・水分のノイズに強く、外れ値に引っ張られない。
-- Trino に厳密な median が無いので、ソート済み配列の中央2要素の平均で出す
-- （奇数件なら同じ要素を2回参照するので実質そのまま）。
gh_daily AS (
    SELECT
        COALESCE(w.dt, f.dt) AS dt,
        w.median_weight_kg,
        f.median_fat_pct
    FROM (
        SELECT
            dt,
            ROUND(
                (
                    element_at(array_sort(array_agg(grams)), (COUNT(*) + 1) / 2)
                    + element_at(array_sort(array_agg(grams)), COUNT(*) / 2 + 1)
                ) / 2000.0,
                1
            ) AS median_weight_kg
        FROM gh_weight
        GROUP BY dt
    ) w
    FULL OUTER JOIN (
        SELECT
            dt,
            ROUND(
                (
                    element_at(array_sort(array_agg(pct)), (COUNT(*) + 1) / 2)
                    + element_at(array_sort(array_agg(pct)), COUNT(*) / 2 + 1)
                ) / 2.0,
                1
            ) AS median_fat_pct
        FROM gh_fat
        GROUP BY dt
    ) f ON w.dt = f.dt
)

SELECT
    p.dt,
    p.steps,
    p.calories_out,
    p.activity_calories,
    p.calories_bmr,
    p.floors,
    p.elevation,
    p.sedentary_minutes,
    p.lightly_active_minutes,
    p.fairly_active_minutes,
    p.very_active_minutes,
    p.total_minutes_asleep,
    p.total_time_in_bed,
    p.sleep_deep_minutes,
    p.sleep_light_minutes,
    p.sleep_rem_minutes,
    p.sleep_wake_minutes,
    p.resting_heart_rate,

    -- Google Health を優先し、無い日はレガシーの値を残す。
    -- 単純に差し替えると、Google Health の bronze がまだ無い過去日
    -- （取り込みは 2026-09-05 以降から）の体重が再処理窓の中で消える。
    COALESCE(g.median_weight_kg, p.legacy_weight_kg) AS weight_kg,
    COALESCE(g.median_fat_pct, p.legacy_body_fat_pct) AS body_fat_pct,

    -- BMI は Google Health API に無いので体重と身長から計算する。
    -- 身長は dbt_project.yml の var（既存データから逆算した実測値）。
    COALESCE(
        ROUND(g.median_weight_kg / POWER({{ var('height_m') }}, 2), 2),
        p.legacy_bmi
    ) AS bmi

FROM parsed p
LEFT JOIN gh_daily g ON g.dt = p.dt
