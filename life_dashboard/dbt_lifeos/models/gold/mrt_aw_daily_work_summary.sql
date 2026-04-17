{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='target_date',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        CAST(event_date_jst AS DATE) AS target_date,
        cat_main, raw_app_name, is_afk,
        CAST(start_ts AS TIMESTAMP(3)) AS st,
        CAST(end_ts AS TIMESTAMP(3)) AS et,
        date_diff('second', CAST(start_ts AS TIMESTAMP(3)), CAST(end_ts AS TIMESTAMP(3))) AS duration_sec
    FROM {{ ref('int_all_behavior_events') }}
    WHERE source_system = 'activitywatch'
    {% if is_incremental() %}
    AND CAST(event_date_jst AS DATE) >= date_add('day', -7, CAST((SELECT MAX(target_date) FROM {{ this }}) AS DATE))
    {% endif %}
),
core_events AS (
    SELECT * FROM base
    WHERE cat_main IN ('WORK', 'DEVELOP') 
      AND is_afk = false
      AND duration_sec > 5
),
lagged AS (
    SELECT *, LAG(et) OVER(PARTITION BY target_date, cat_main ORDER BY st) as prev_et
    FROM core_events
),
session_flags AS (
    SELECT *,
        CASE
            WHEN prev_et IS NULL THEN 1
            WHEN date_diff('second', prev_et, st) > 3000 THEN 1
            ELSE 0
        END AS is_new_session
    FROM lagged
),
session_assigned AS (
    SELECT *, SUM(is_new_session) OVER(PARTITION BY target_date, cat_main ORDER BY st) AS session_id
    FROM session_flags
),
session_boundaries AS (
    SELECT target_date, cat_main, session_id,
        MIN(st) AS session_start, MAX(et) AS session_end,
        date_diff('second', MIN(st), MAX(et)) AS session_duration_sec
    FROM session_assigned
    GROUP BY target_date, cat_main, session_id
),
daily_stats AS (
    SELECT target_date,
        SUM(IF(cat_main = 'WORK', session_duration_sec, 0)) AS work_session_sec,
        SUM(IF(cat_main = 'DEVELOP', session_duration_sec, 0)) AS dev_session_sec
    FROM session_boundaries
    GROUP BY target_date
),
core_stats AS (
    SELECT target_date,
        SUM(IF(cat_main = 'WORK', duration_sec, 0)) AS work_core_sec,
        SUM(IF(cat_main = 'DEVELOP', duration_sec, 0)) AS dev_core_sec
    FROM core_events
    GROUP BY target_date
),
app_usage_agg AS (
    SELECT target_date, cat_main, raw_app_name, SUM(duration_sec) AS app_duration_sec
    FROM core_events
    GROUP BY target_date, cat_main, raw_app_name
),
app_usage_ranked AS (
    SELECT target_date, cat_main, raw_app_name, app_duration_sec,
        row_number() OVER(PARTITION BY target_date, cat_main ORDER BY app_duration_sec DESC) as rn
    FROM app_usage_agg
),
app_usage_pivot AS (
    SELECT target_date,
        MAX(IF(cat_main = 'WORK', apps_str)) AS work_apps_str,
        MAX(IF(cat_main = 'DEVELOP', apps_str)) AS dev_apps_str
    FROM (
        SELECT target_date, cat_main,
            ARRAY_JOIN(ARRAY_AGG(raw_app_name || ':' || CAST(app_duration_sec AS VARCHAR) ORDER BY app_duration_sec DESC), '||') AS apps_str
        FROM app_usage_ranked WHERE rn <= 10 GROUP BY target_date, cat_main
    )
    GROUP BY target_date
),
joined_stats AS (
    SELECT
        c.target_date,
        COALESCE(c.work_core_sec, 0) AS work_core_sec,
        COALESCE(d.work_session_sec, 0) AS work_session_sec,
        COALESCE(c.dev_core_sec, 0) AS dev_core_sec,
        COALESCE(d.dev_session_sec, 0) AS dev_session_sec,
        COALESCE(a.work_apps_str, '') AS work_apps_str,
        COALESCE(a.dev_apps_str, '') AS dev_apps_str
    FROM core_stats c
    LEFT JOIN daily_stats d ON c.target_date = d.target_date
    LEFT JOIN app_usage_pivot a ON c.target_date = a.target_date
),
scored AS (
    SELECT
        *,
        -- 集中度(%)の計算
        IF(work_session_sec > 0, ROUND(CAST(work_core_sec AS DOUBLE) / work_session_sec * 100), 0) AS work_focus_rate,
        IF(dev_session_sec > 0, ROUND(CAST(dev_core_sec AS DOUBLE) / dev_session_sec * 100), 0) AS dev_focus_rate,
        -- ベーススコア (Workは8時間=28800秒, Devは4時間=14400秒を分母にする)
        CAST(work_core_sec AS DOUBLE) / 28800 * 100 AS work_base_score,
        CAST(dev_core_sec AS DOUBLE) / 14400 * 100 AS dev_base_score
    FROM joined_stats
)
-- 最終結合とボーナス倍率の適用（最大120点でキャップ）
SELECT
    target_date,
    work_core_sec, work_session_sec, CAST(work_focus_rate AS INTEGER) AS work_focus_rate,
    CAST(LEAST(ROUND(work_base_score * CASE
        WHEN work_focus_rate >= 70 THEN 1.2
        WHEN work_focus_rate >= 60 THEN 1.1
        WHEN work_focus_rate >= 40 THEN 1.0
        WHEN work_focus_rate >= 30 THEN 0.5
        ELSE 0.1 END), 120) AS INTEGER) AS work_score,
    work_apps_str,
    dev_core_sec, dev_session_sec, CAST(dev_focus_rate AS INTEGER) AS dev_focus_rate,
    CAST(LEAST(ROUND(dev_base_score * CASE
        WHEN dev_focus_rate >= 70 THEN 1.2
        WHEN dev_focus_rate >= 60 THEN 1.1
        WHEN dev_focus_rate >= 40 THEN 1.0
        WHEN dev_focus_rate >= 30 THEN 0.5
        ELSE 0.1 END), 120) AS INTEGER) AS dev_score,
    dev_apps_str
FROM scored
