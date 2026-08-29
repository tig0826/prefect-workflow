{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='time_slot_jst',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['slot_date_jst']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH bounds AS (
    SELECT
        CAST(date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_start_local,
        CAST(date_add('day', 1, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_end_local
),

time_spine AS (
    SELECT
        slot_start_local AS time_slot_jst,
        (slot_start_local + INTERVAL '15' MINUTE) AS time_slot_end_jst
    FROM bounds b
    CROSS JOIN UNNEST(sequence(b.window_start_local, b.window_end_local - INTERVAL '15' MINUTE, INTERVAL '15' MINUTE)) AS t(slot_start_local)
),

events AS (
    SELECT *
    FROM {{ ref('int_all_behavior_events') }}
    WHERE NOT is_afk
    {% if is_incremental() %}
      AND start_ts < (SELECT window_end_local FROM bounds)
      AND end_ts > (SELECT window_start_local FROM bounds)
    {% endif %}
),

overlaps AS (
    SELECT
        s.time_slot_jst,
        s.time_slot_end_jst,
        e.cat_main,
        e.cat_sub,
        e.priority,
        date_diff('second', GREATEST(s.time_slot_jst, e.start_ts), LEAST(s.time_slot_end_jst, e.end_ts)) AS overlap_sec
    FROM time_spine s
    JOIN events e
      ON e.start_ts < s.time_slot_end_jst
     AND e.end_ts > s.time_slot_jst
),

sub_aggregated AS (
    SELECT time_slot_jst, time_slot_end_jst, cat_main, cat_sub, priority, SUM(overlap_sec) AS sub_overlap_sec
    FROM overlaps
    WHERE overlap_sec > 0
    GROUP BY 1,2,3,4,5
),

-- 帯（1スロット1ラベル）の勝者判定には外出先滞在を入れない。
-- 13時間の合宿のような長い滞在があると、活動ログが無いスロットだけで
-- OUTING が勝ち、帯が緑と他の色でまばらに切り替わって読めなくなる。
-- 「外出していた」という事実は下の is_outing 下線レイヤーが持つ。
band_candidates AS (
    SELECT * FROM sub_aggregated WHERE cat_main <> 'OUTING'
),

main_aggregated AS (
    SELECT
        *,
        SUM(sub_overlap_sec) OVER (PARTITION BY time_slot_jst, cat_main) AS main_overlap_sec,
        SUM(CASE WHEN priority >= 20 THEN sub_overlap_sec ELSE 0 END) OVER (PARTITION BY time_slot_jst) AS active_total_sec
    FROM band_candidates
),

ranked_observed AS (
    SELECT
        time_slot_jst,
        time_slot_end_jst,
        cat_main,
        cat_sub,
        LEAST(CAST(sub_overlap_sec AS bigint), BIGINT '900') AS overlap_sec,
        ROW_NUMBER() OVER (
            PARTITION BY time_slot_jst
            ORDER BY
                CASE
                    WHEN active_total_sec >= 60 AND priority >= 20 THEN 1
                    WHEN active_total_sec >= 60 AND priority < 20 THEN 0
                    ELSE 1
                END DESC,
                (main_overlap_sec * priority) DESC,
                sub_overlap_sec DESC,
                priority DESC,
                cat_main,
                cat_sub
        ) AS rn
    FROM main_aggregated
),

observed_winners AS (
    SELECT time_slot_jst, time_slot_end_jst, cat_main, cat_sub, overlap_sec
    FROM ranked_observed
    WHERE rn = 1
),

unobserved_slots AS (
    SELECT s.time_slot_jst, s.time_slot_end_jst, 'UNOBSERVED' AS cat_main, 'データなし' AS cat_sub, BIGINT '900' AS overlap_sec
    FROM time_spine s
    LEFT JOIN observed_winners o ON s.time_slot_jst = o.time_slot_jst
    WHERE o.time_slot_jst IS NULL
),

final_slots AS (
    SELECT * FROM observed_winners
    UNION ALL
    SELECT * FROM unobserved_slots
),

-- 「移動していたか」は帯の勝者ラベルとは独立に持つ。
-- 帯グラフは1スロット1ラベルなので priority 勝負にすると、移動中に
-- スマホを触った瞬間に移動の帯が途切れる。移動は画面の使い方と排他ではない
-- （移動しながら音楽を聴いても移動は続いている）ので、UI が連続した
-- 移動レイヤーを描けるようフラグとして出す。
--
-- ★対象は TRANSIT だけ。OUTING（外出先滞在）を含めてはいけない★
-- 滞在まで含めると実家に数日帰省した期間が丸ごと「移動」の下線で
-- 埋まってしまう。動いていた時間だけが移動である。
transit_context AS (
    SELECT
        time_slot_jst,
        SUM(sub_overlap_sec) AS transit_sec,
        MAX(cat_sub) AS transit_sub
    FROM sub_aggregated
    WHERE cat_main = 'TRANSIT'
    GROUP BY time_slot_jst
),

-- 外出先滞在も同じく下線レイヤーとして出す。
-- ★TRANSIT と混ぜてはいけない★ 混ぜると実家に数日帰省した期間が
-- 丸ごと「移動」に見える。UI は2色で描き分ける。
outing_context AS (
    SELECT
        time_slot_jst,
        SUM(sub_overlap_sec) AS outing_sec
    FROM sub_aggregated
    WHERE cat_main = 'OUTING'
    GROUP BY time_slot_jst
)

SELECT
    CAST(f.time_slot_jst AS timestamp) AS time_slot_jst,
    CAST(f.time_slot_end_jst AS timestamp) AS time_slot_end_jst,
    CAST(f.time_slot_jst AS date) AS slot_date_jst,
    f.cat_main,
    f.cat_sub,
    f.overlap_sec,
    -- 60秒未満のかすりは対象にしない（スロット境界のノイズ除去）
    COALESCE(tc.transit_sec, 0) >= 60 AS is_transit,
    CASE WHEN COALESCE(tc.transit_sec, 0) >= 60 THEN tc.transit_sub END AS transit_kind,
    COALESCE(oc.outing_sec, 0) >= 60 AS is_outing,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS timestamp) AS transformed_at_jst
FROM final_slots f
LEFT JOIN transit_context tc ON tc.time_slot_jst = f.time_slot_jst
LEFT JOIN outing_context oc ON oc.time_slot_jst = f.time_slot_jst
WHERE f.time_slot_jst < CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS timestamp)
