{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='target_date',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['target_date']
) }}

-- 1. Fitbitの日次サマリー（詳細データと体重をすべて取得）
WITH fitbit_daily AS (
    SELECT
        CAST(dt AS DATE) AS target_date,
        COALESCE(steps, 0) AS steps,
        COALESCE(calories_out, 0) AS calories_out,
        COALESCE(resting_heart_rate, 0) AS resting_heart_rate,
        
        -- 活動の質
        activity_calories,
        calories_bmr,
        sedentary_minutes,
        lightly_active_minutes,
        fairly_active_minutes,
        very_active_minutes,
        
        -- 睡眠の質
        total_minutes_asleep,
        total_time_in_bed,
        sleep_deep_minutes,
        sleep_light_minutes,
        sleep_rem_minutes,
        sleep_wake_minutes,
        
        -- 体組成 (0はNULLに変換して平均計算のバグを防ぐ)
        NULLIF(weight_kg, 0) AS weight_kg,
        NULLIF(body_fat_pct, 0) AS body_fat_pct,
        NULLIF(bmi, 0) AS bmi
    FROM {{ ref('fitbit_summary') }}
    {% if is_incremental() %}
    WHERE CAST(dt AS DATE) >= date_add('day', -14, current_date)
    {% endif %}
),

-- 2. Fitbitの運動ログ（圧縮）
fitbit_activities_aggregated AS (
    SELECT
        CAST(dt AS DATE) AS target_date,
        activity_name,
        SUM(duration_sec) AS total_duration_sec,
        SUM(steps) AS total_steps,
        SUM(calories) AS total_calories
    FROM {{ ref('fitbit_activity') }}
    {% if is_incremental() %}
    -- 直近14日分だけ再計算してマージ（Icebergの効率化）
    WHERE CAST(dt AS DATE) >= date_add('day', -14, current_date)
    {% endif %}
    GROUP BY CAST(dt AS DATE), activity_name
),

fitbit_activities AS (
    SELECT
        target_date,
        ARRAY_JOIN(ARRAY_AGG(
            '[' || activity_name || '] ' || 
            CAST(ROUND(total_duration_sec / 60.0) AS VARCHAR) || 'm (' || 
            CAST(total_steps AS VARCHAR) || ' steps, ' || 
            CAST(total_calories AS VARCHAR) || 'kcal)'
            ORDER BY total_duration_sec DESC -- 運動時間が長い順に並べる
        ), ' || ') AS activity_logs_str
    FROM fitbit_activities_aggregated
    GROUP BY target_date
),
-- 3. Askenの食事データ
asken_daily AS (
    SELECT
        meal_date AS target_date,
        COALESCE(calories_kcal, 0) AS calories_in
    FROM {{ ref('asken_nutrition') }}
    {% if is_incremental() %}
    WHERE meal_date >= date_add('day', -14, current_date)
    {% endif %}
),

-- 4. 結合
joined_base AS (
    SELECT
        COALESCE(f.target_date, a.target_date) AS target_date,
        COALESCE(f.steps, 0) AS steps,
        COALESCE(f.calories_out, 0) AS calories_out,
        COALESCE(a.calories_in, 0) AS calories_in,
        
        -- カロリー差引 (摂取 - 消費)
        COALESCE(a.calories_in, 0) - COALESCE(f.calories_out, 0) AS net_calorie_balance,
        
        -- Fitbitの詳細データを通過させる
        f.activity_calories,
        f.calories_bmr,
        f.sedentary_minutes,
        f.lightly_active_minutes,
        f.fairly_active_minutes,
        f.very_active_minutes,
        f.total_minutes_asleep,
        f.total_time_in_bed,
        f.sleep_deep_minutes,
        f.sleep_light_minutes,
        f.sleep_rem_minutes,
        f.sleep_wake_minutes,
        COALESCE(f.resting_heart_rate, 0) AS resting_heart_rate,
        f.weight_kg,
        f.body_fat_pct,
        f.bmi,

        COALESCE(act.activity_logs_str, '') AS activity_logs_str
    FROM fitbit_daily f
    FULL OUTER JOIN asken_daily a ON f.target_date = a.target_date
    LEFT JOIN fitbit_activities act ON COALESCE(f.target_date, a.target_date) = act.target_date
),

-- 5. 移動平均の算出
moving_averages AS (
    SELECT
        *,
        -- 過去7日間の平均体重（NULLは自動で無視される）
        AVG(weight_kg) OVER (
            ORDER BY target_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weight_7d_avg,
        
        -- 過去7日間の平均カロリー収支
        AVG(net_calorie_balance) OVER (
            ORDER BY target_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS net_calorie_7d_avg,

        -- 過去7日間の安静時心拍数（疲労度のトレンド分析用）
        AVG(resting_heart_rate) OVER (
            ORDER BY target_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS resting_hr_7d_avg
    FROM joined_base
)

SELECT * FROM moving_averages
