{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='route_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['route_date']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}
{# A real outing samples every ~5 min (12 min at the 90th percentile). Anything past
   this is Google having stopped recording, i.e. a genuine break between outings. #}
{% set track_break_minutes = 30 %}

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

-- Google emits timelinePath in fixed 2-hour buckets. Grouping by bucket draws one
-- polyline per bucket, so a single journey arrives as disconnected fragments with a
-- visible gap (up to ~1 km) wherever it crosses a boundary -- a round trip home never
-- looks like a closed loop. Concatenate the whole day and cut only on a real sampling
-- gap instead, so one outing renders as one continuous track.
deduped AS (
    SELECT event_date_jst, waypoint_time_jst, lat, lng
    FROM (
        SELECT
            event_date_jst,
            waypoint_time_jst,
            lat,
            lng,
            -- adjacent buckets can repeat the point that sits on their shared boundary
            ROW_NUMBER() OVER (
                PARTITION BY event_date_jst, waypoint_time_jst ORDER BY lat, lng
            ) AS rn
        FROM paths
    )
    WHERE rn = 1
),

with_break AS (
    SELECT
        *,
        CASE
            WHEN date_diff(
                     'minute',
                     LAG(waypoint_time_jst) OVER (
                         PARTITION BY event_date_jst ORDER BY waypoint_time_jst
                     ),
                     waypoint_time_jst
                 ) > {{ track_break_minutes }}
            THEN 1
            ELSE 0
        END AS is_break
    FROM deduped
),

tracks AS (
    SELECT
        *,
        SUM(is_break) OVER (
            PARTITION BY event_date_jst
            ORDER BY waypoint_time_jst
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS track_id
    FROM with_break
),

-- Google keeps emitting points after you stop moving, so a track that ends at home
-- drags a tail of identical coordinates for hours. They add nothing to the polyline
-- and push ended_at past midnight; collapse each stationary run to its first point.
-- 25 m clears the ~15 m of GPS jitter seen while parked and is far below the ~290 m
-- median step between points while moving, so no real movement is eaten.
moving AS (
    SELECT event_date_jst, track_id, waypoint_time_jst, lat, lng
    FROM (
        SELECT
            *,
            LAG(lat) OVER (
                PARTITION BY event_date_jst, track_id ORDER BY waypoint_time_jst
            ) AS prev_lat,
            LAG(lng) OVER (
                PARTITION BY event_date_jst, track_id ORDER BY waypoint_time_jst
            ) AS prev_lng
        FROM tracks
    )
    WHERE prev_lat IS NULL
       OR ST_Distance(
              to_spherical_geography(ST_Point(lng, lat)),
              to_spherical_geography(ST_Point(prev_lng, prev_lat))
          ) > 25
),

route_aggregated AS (
    SELECT
        event_date_jst,
        track_id,
        MIN(waypoint_time_jst) AS track_start_jst,
        MAX(waypoint_time_jst) AS track_end_jst,
        COUNT(*) AS point_count,
        CAST(json_format(CAST(
            array_agg(
                json_parse('{"lat":' || CAST(lat AS VARCHAR) || ',"lng":' || CAST(lng AS VARCHAR) || '}')
                ORDER BY waypoint_time_jst
            ) AS JSON
        )) AS VARCHAR) AS route_json
    FROM moving
    GROUP BY event_date_jst, track_id
    HAVING COUNT(*) >= 2
      -- every point within ~55 m: sitting still all day, not a route
      AND (MAX(lat) - MIN(lat) > 0.0005 OR MAX(lng) - MIN(lng) > 0.0005)
),

-- A track usually spans several legs (walk -> bus -> walk). Label it with the leg that
-- covered the most ground, and report the distance of the whole track.
track_mode AS (
    SELECT
        event_date_jst,
        track_id,
        MAX_BY(activity_type, COALESCE(type_distance, 0)) AS activity_type,
        SUM(type_distance) AS distance_meters
    FROM (
        SELECT
            r.event_date_jst,
            r.track_id,
            a.activity_type,
            SUM(a.distance_meters) AS type_distance
        FROM route_aggregated r
        JOIN activities a
          ON a.start_ts_jst < r.track_end_jst
         AND a.end_ts_jst   > r.track_start_jst
        GROUP BY r.event_date_jst, r.track_id, a.activity_type
    )
    GROUP BY event_date_jst, track_id
)

SELECT
    to_hex(md5(to_utf8(
        CAST(r.event_date_jst AS VARCHAR) || '|' ||
        CAST(r.track_start_jst AS VARCHAR) || '|' ||
        CAST(r.track_end_jst AS VARCHAR)
    ))) AS route_pk,
    r.event_date_jst AS route_date,
    COALESCE(m.activity_type, 'UNKNOWN') AS transport_mode,
    CAST(r.track_start_jst AS TIMESTAMP) AS started_at,
    CAST(r.track_end_jst AS TIMESTAMP) AS ended_at,
    m.distance_meters,
    r.route_json,
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM route_aggregated r
LEFT JOIN track_mode m
  ON m.event_date_jst = r.event_date_jst
 AND m.track_id = r.track_id
