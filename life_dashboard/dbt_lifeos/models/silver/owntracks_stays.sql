{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stay_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['day(stay_start_time)']
) }}

WITH base AS (
    SELECT
        tracker_id,
        event_time_jst,
        latitude,
        longitude
    FROM {{ ref('owntracks_locations') }}
    {% if is_incremental() %}
    -- 前回の滞在セッションを復元するため、少し深めにルックバック
    WHERE event_time_jst >= date_add('day', -3, current_date)
    {% endif %}
),

state_changes AS (
    SELECT
        *,
        LAG(event_time_jst) OVER (PARTITION BY tracker_id ORDER BY event_time_jst) AS prev_time,
        LAG(latitude) OVER (PARTITION BY tracker_id ORDER BY event_time_jst) AS prev_lat,
        LAG(longitude) OVER (PARTITION BY tracker_id ORDER BY event_time_jst) AS prev_lon,
        -- 🌟 アプローチBの核心: 「次のPingが飛んだ時刻」をカンニングする
        LEAD(event_time_jst) OVER (PARTITION BY tracker_id ORDER BY event_time_jst) AS next_time
    FROM base
),

session_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_time IS NULL THEN 1
            
            -- 🌟 修正点1: 時間での切断（30分）を廃止。
            -- モニタリングモードにおいて「同じ場所での沈黙」は滞在の証拠拠。
            -- 空間的に 100m 以上動いた時のみ、セッションを切る。
            WHEN ST_Distance(
                to_spherical_geography(ST_Point(prev_lon, prev_lat)),
                to_spherical_geography(ST_Point(longitude, latitude))
            ) > 100.0 THEN 1
            
            -- 例外: さすがに12時間以上データが飛んだら端末の電源切れとみなして切る
            WHEN date_diff('hour', prev_time, event_time_jst) > 12 THEN 1
            
            ELSE 0
        END AS is_new_session
    FROM state_changes
),

session_ids AS (
    SELECT
        *,
        SUM(is_new_session) OVER (PARTITION BY tracker_id ORDER BY event_time_jst) AS temp_session_id
    FROM session_flags
),

stay_sessions AS (
    SELECT
        tracker_id,
        MIN(event_time_jst) AS stay_start_time,
        MAX(event_time_jst) AS raw_end_time,
        -- 🌟 修正点2: 1ポイントしかない滞在でも、次のPing（移動開始）までを滞在とみなす
        MAX(COALESCE(next_time, event_time_jst)) AS inferred_end_time,
        AVG(latitude) AS centroid_latitude,
        AVG(longitude) AS centroid_longitude,
        COUNT(*) AS point_count
    FROM session_ids
    GROUP BY tracker_id, temp_session_id
)

SELECT
    to_hex(md5(to_utf8(
        tracker_id || '|' || to_iso8601(stay_start_time)
    ))) AS stay_pk,

    tracker_id,
    stay_start_time,
    -- 実測値ではなく、推測値（次の移動開始前まで）を終了時間とする
    inferred_end_time AS stay_end_time,
    -- 推測された滞在時間を計算
    date_diff('minute', stay_start_time, inferred_end_time) AS duration_minutes,
    
    centroid_latitude,
    centroid_longitude,
    point_count,

    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM stay_sessions
-- 🌟 修正点3: ここで初めて5分未満の「信号待ちレベルのノイズ」を弾く
WHERE date_diff('minute', stay_start_time, inferred_end_time) >= 5
