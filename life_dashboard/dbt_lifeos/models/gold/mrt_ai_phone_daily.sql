{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='target_date',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['target_date']
) }}

-- スマホの使い方から「注意の断片化」を測る日次マート。
--
-- なぜ必要か:
--   本人が自分では絶対に数えられない数字だから。実測で解錠 32〜67回/日、
--   うち深夜 0-4時 に 5〜13回。これは睡眠が細切れに中断されている物理的な証拠で、
--   「概日リズムが乱れています」という抽象論を、観測可能な事実に置き換えられる。
--   mrt_behavior_slots_15m からは原理的に出てこない（15分スロットの勝者1カテゴリ
--   しか持たないので、瞬間的な解錠は消える）。
--
-- unlock_gap_* は解錠の間隔から出す指標。夜間に長い無操作区間が1本あるのが
-- 健全な形で、短い区間が並ぶのが断片化。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH unlocks AS (
    SELECT
        event_date_jst,
        unlock_ts_jst,
        unlock_hour_jst,
        LAG(unlock_ts_jst) OVER (ORDER BY unlock_ts_jst) AS prev_unlock_ts
    FROM {{ ref('aw_unlock_events') }}
    {% if is_incremental() %}
    WHERE unlock_ts_jst >= date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo'))
    {% endif %}
),

with_gap AS (
    SELECT *,
        date_diff('minute', prev_unlock_ts, unlock_ts_jst) AS gap_min
    FROM unlocks
),

daily AS (
    SELECT
        event_date_jst AS target_date,
        COUNT(*) AS unlock_count,
        -- 深夜帯の解錠。睡眠の中断の指標
        COUNT_IF(unlock_hour_jst BETWEEN 0 AND 4) AS unlock_count_00_04,
        -- 就業時間帯の解錠。作業の中断の指標
        COUNT_IF(unlock_hour_jst BETWEEN 9 AND 18) AS unlock_count_09_18,
        -- 夜の解錠。就寝前のスマホ
        COUNT_IF(unlock_hour_jst BETWEEN 22 AND 23) AS unlock_count_22_23,
        MIN(unlock_hour_jst) AS first_unlock_hour,
        MAX(unlock_hour_jst) AS last_unlock_hour,
        -- その日で最も長かった「スマホを触らなかった」区間（分）。
        -- 夜間睡眠が取れている日はここが長くなる。
        MAX(gap_min) AS longest_no_unlock_gap_min,
        -- 5分以内の連続解錠。落ち着かずに何度も開いている状態
        COUNT_IF(gap_min IS NOT NULL AND gap_min <= 5) AS rapid_reunlock_count
    FROM with_gap
    GROUP BY event_date_jst
)

SELECT
    target_date,
    unlock_count,
    unlock_count_00_04,
    unlock_count_09_18,
    unlock_count_22_23,
    first_unlock_hour,
    last_unlock_hour,
    longest_no_unlock_gap_min,
    rapid_reunlock_count,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM daily
