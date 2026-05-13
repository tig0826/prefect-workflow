{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='route_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['route_date']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH paths AS (
    SELECT *
    FROM {{ ref('timeline_paths') }}
    {% if is_incremental() %}
    WHERE event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS DATE)
    {% endif %}
),

activities AS (
    SELECT *
    FROM {{ ref('timeline_segments') }}
    WHERE segment_type = 'activity'
    {% if is_incremental() %}
      AND event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS DATE)
    {% endif %}
),

-- timelinePath セグメント単位で waypoint を集約（home 付近を除く）
route_aggregated AS (
    SELECT
        event_date_jst,
        segment_start_jst,
        segment_end_jst,
        COUNT(*) AS point_count,
        -- 移動量を確認（最初と最後の点の距離が極めて小さい場合は自宅滞在とみなす）
        MAX(lat) - MIN(lat) AS lat_spread,
        MAX(lng) - MIN(lng) AS lng_spread,
        CAST(json_format(CAST(
            array_agg(
                json_parse('{"lat":' || CAST(lat AS VARCHAR) || ',"lng":' || CAST(lng AS VARCHAR) || '}')
                ORDER BY waypoint_time_jst
            ) AS JSON
        )) AS VARCHAR) AS route_json
    FROM paths
    GROUP BY event_date_jst, segment_start_jst, segment_end_jst
    HAVING COUNT(*) >= 2
      -- 全点がほぼ同座標（自宅待機など）のブロックを除外
      AND (MAX(lat) - MIN(lat) > 0.0005 OR MAX(lng) - MIN(lng) > 0.0005)
),

-- 対応する activity から transport_mode を取得（time overlap で LEFT JOIN）
with_mode AS (
    SELECT
        r.event_date_jst,
        r.segment_start_jst,
        r.segment_end_jst,
        r.point_count,
        r.route_json,
        a.activity_type,
        a.distance_meters,
        ROW_NUMBER() OVER (
            PARTITION BY r.segment_start_jst, r.segment_end_jst
            ORDER BY COALESCE(a.activity_type, 'z')
        ) AS rn
    FROM route_aggregated r
    LEFT JOIN activities a
      ON a.start_ts_jst < r.segment_end_jst
     AND a.end_ts_jst   > r.segment_start_jst
)

SELECT
    to_hex(md5(to_utf8(
        CAST(segment_start_jst AS VARCHAR) || '|' || CAST(segment_end_jst AS VARCHAR)
    ))) AS route_pk,
    event_date_jst AS route_date,
    COALESCE(activity_type, 'UNKNOWN') AS transport_mode,
    CAST(segment_start_jst AS TIMESTAMP) AS started_at,
    CAST(segment_end_jst AS TIMESTAMP) AS ended_at,
    distance_meters,
    route_json,
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM with_mode
WHERE rn = 1
