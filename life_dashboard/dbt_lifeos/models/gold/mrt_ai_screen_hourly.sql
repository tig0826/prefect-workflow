{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='slot_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['activity_date_jst']
) }}

-- 「画面を見ていた時間」を **分単位で重複排除** した日 × 時のマート。
--
-- なぜ必要か:
--   mrt_ai_activity_hourly は source（window / media / web）を分けて持つのが正しいが、
--   合算すると同じ行動が二重に数えられる。実際に週次FBの LLM が
--   `source IN ('window','web')` で合算した metric を書き、
--   0-4時の画面時間を 258.4分/日 と報告した（真の値は 210.6分/日、23%過大）。
--   モデル内にコメントで警告していても、クエリを書く側が毎回正しく扱う前提は破綻する。
--
--   → 「重複のない画面時間」という定義を1箇所に置き、それを参照させる。
--     これは _correlation の n=4 問題と同じ構造の対策（定義を分散させない）。
--
-- 定義:
--   ・対象は「画面に向かっていた」と言える活動（睡眠は除く）
--   ・端末をまたいでも同じ1分は1分として数える
--     （PCで開発しながらスマホで再生していても 1分）
--   ・NOT is_afk で放置ウィンドウを除外
--
-- 端末別に見たいときや「何を見ていたか」を知りたいときは
-- mrt_ai_activity_hourly を使う。こちらは総量だけを正確に出すためのもの。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH bounds AS (
    SELECT
        CAST(date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_start_local,
        CAST(date_add('day', 1, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_end_local
),

screen_events AS (
    -- 前面にあったアプリ／ウィンドウ
    SELECT start_ts, end_ts, cat_main
    FROM {{ ref('int_aw_categorized') }}
    WHERE NOT is_afk

    UNION ALL

    -- 実際に再生されていたもの（画面を消して音楽だけ、も含まれる点は許容）
    SELECT start_ts, end_ts, cat_main
    FROM {{ ref('int_aw_media') }}

    UNION ALL

    -- スマホのブラウザ
    SELECT start_ts, end_ts, cat_main
    FROM {{ ref('int_aw_web') }}
),

scoped AS (
    SELECT e.*
    FROM screen_events e
    WHERE e.end_ts > e.start_ts
    {% if is_incremental() %}
      AND e.start_ts < (SELECT window_end_local FROM bounds)
      AND e.end_ts > (SELECT window_start_local FROM bounds)
    {% endif %}
),

-- 分粒度に展開してから DISTINCT を取ることで、端末やソースをまたいだ
-- 重複を潰す。1イベントあたり最大でも数百分なので展開コストは許容範囲。
minutes AS (
    SELECT DISTINCT
        date_trunc('minute', m) AS minute_ts,
        cat_main
    FROM scoped
    CROSS JOIN UNNEST(sequence(
        date_trunc('minute', start_ts),
        date_trunc('minute', end_ts),
        INTERVAL '1' MINUTE
    )) AS t(m)
    WHERE m < end_ts
),

distinct_minutes AS (
    -- カテゴリをまたいだ重複も潰した「画面に向かっていた分」
    SELECT DISTINCT minute_ts FROM minutes
),

aggregated AS (
    SELECT
        CAST(minute_ts AS DATE) AS activity_date_jst,
        hour(minute_ts) AS hour_jst,
        COUNT(*) AS screen_minutes
    FROM distinct_minutes
    GROUP BY 1, 2
),

-- 参考情報として、その時間帯で最も長かったカテゴリも持たせる
top_cat AS (
    SELECT activity_date_jst, hour_jst, cat_main AS top_cat_main, cat_minutes
    FROM (
        SELECT
            CAST(minute_ts AS DATE) AS activity_date_jst,
            hour(minute_ts) AS hour_jst,
            cat_main,
            COUNT(*) AS cat_minutes,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(minute_ts AS DATE), hour(minute_ts)
                ORDER BY COUNT(*) DESC, cat_main
            ) AS rn
        FROM minutes
        GROUP BY 1, 2, 3
    )
    WHERE rn = 1
)

SELECT
    to_hex(md5(to_utf8(
        CAST(a.activity_date_jst AS VARCHAR) || '|' || CAST(a.hour_jst AS VARCHAR)
    ))) AS slot_pk,
    a.activity_date_jst,
    a.hour_jst,
    a.screen_minutes,
    t.top_cat_main,
    t.cat_minutes AS top_cat_minutes,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM aggregated a
LEFT JOIN top_cat t
  ON a.activity_date_jst = t.activity_date_jst AND a.hour_jst = t.hour_jst
WHERE a.screen_minutes > 0
