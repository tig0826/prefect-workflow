{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='target_date',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['target_date']
) }}

-- ★7日移動平均は、出力する期間より6日前から読んで計算する（2026-10-10 修正）★
-- 以前は incremental の実行で「直近14日」だけを読み、その中で7日窓を計算していた。
-- そのため読み込み範囲の先頭の日は、窓の中に自分しか無い状態で計算され merge で上書きされる。
-- 日が経って先頭に来た日ほど窓が欠け、最後は「7日平均＝その日の値」で固定されていた
-- （実測: 9/28 以前の weight_7d_avg はすべて当日の体重と一致、weight_7d_n=1）。
-- 安静時心拍・カロリー収支の7日平均も同じ壊れ方をしていた。
-- 読み込みは 14+6 日前から、出力は 14 日分だけにして、窓が常に7日そろうようにする。
-- 過去分を直すには一度だけ --full-refresh が必要（incremental は14日分しか作り直さない）。
-- reprocess_days は他モデルと同じ var で上書きできる。gold には既定で 7 が渡るが、
-- 従来どおり最低14日は作り直す。
{% set reprocess_days = [var('reprocess_days', 14) | int, 14] | max %}
{% set lookback_days = reprocess_days + 6 %}

-- 1. Fitbitの日次サマリー（詳細データと体重をすべて取得）
WITH fitbit_daily AS (
    SELECT
        CAST(dt AS DATE) AS target_date,
        COALESCE(steps, 0) AS steps,
        COALESCE(calories_out, 0) AS calories_out,
        -- ★安静時心拍を 0 で埋めてはいけない★
        -- 0 は「未計測」であって「心拍が0」ではない。COALESCE(...,0) すると
        -- 移動平均に 0 が混ざり、7日平均が 78.5 → 68.6 まで落ちる（実測）。
        -- その結果 AI FB とチャットが「今日は平均より13も高い」と誤って警告していた。
        -- NULL のまま流せば AVG が自動で無視する。
        -- NULLIF も併用して、既に 0 で入っている値も未計測として扱う。
        NULLIF(resting_heart_rate, 0) AS resting_heart_rate,
        
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
    WHERE CAST(dt AS DATE) >= date_add('day', -{{ lookback_days }}, current_date)
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
    -- 直近14日分を再計算してマージ（7日平均のために6日余分に読む）
    WHERE CAST(dt AS DATE) >= date_add('day', -{{ lookback_days }}, current_date)
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
    WHERE meal_date >= date_add('day', -{{ lookback_days }}, current_date)
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
        -- ここも 0 埋め禁止（上と同じ理由）
        NULLIF(f.resting_heart_rate, 0) AS resting_heart_rate,
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

        -- 過去7日間の安静時心拍数（疲労度のトレンド分析用）。
        -- NULL は AVG が自動で無視するので、未計測日は平均を歪めない。
        AVG(resting_heart_rate) OVER (
            ORDER BY target_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS resting_hr_7d_avg,

        -- 何日分で平均したかを併記する。欠測が多い週の平均を
        -- 「7日平均」として扱うと比較が成立しないため、下流が判断できるようにする。
        COUNT(resting_heart_rate) OVER (
            ORDER BY target_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS resting_hr_7d_n,
        COUNT(weight_kg) OVER (
            ORDER BY target_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weight_7d_n
    FROM joined_base
)

SELECT * FROM moving_averages
{% if is_incremental() %}
-- 余分に読んだ6日分は窓を埋めるためだけのもの。書き戻すのは直近14日分だけ
-- （先頭の6日は窓が欠けた値になっているので、書くと正しい過去の値を壊す）。
WHERE target_date >= date_add('day', -{{ reprocess_days }}, current_date)
{% endif %}
