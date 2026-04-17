{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='time_slot_jst',
    table_type='iceberg'
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

main_aggregated AS (
    SELECT
        *,
        SUM(sub_overlap_sec) OVER (PARTITION BY time_slot_jst, cat_main) AS main_overlap_sec,
        SUM(CASE WHEN priority >= 20 THEN sub_overlap_sec ELSE 0 END) OVER (PARTITION BY time_slot_jst) AS active_total_sec
    FROM sub_aggregated
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
)

SELECT
    CAST(time_slot_jst AS timestamp) AS time_slot_jst,
    CAST(time_slot_end_jst AS timestamp) AS time_slot_end_jst,
    CAST(time_slot_jst AS date) AS slot_date_jst,
    cat_main,
    cat_sub,
    overlap_sec,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS timestamp) AS transformed_at_jst
FROM final_slots
WHERE time_slot_jst < CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS timestamp)
