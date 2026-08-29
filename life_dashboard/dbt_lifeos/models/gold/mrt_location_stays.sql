{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stay_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['stay_date']
) }}

{% set reprocess_days = var('reprocess_days', 14) %}
{% set home_lat = 35.81176785 %}
{% set home_lng = 139.72914345 %}
{% set home_radius_m = 150 %}

WITH visits AS (
    SELECT *
    FROM {{ ref('timeline_segments') }}
    -- 自宅も残す。以前は place_semantic_type と距離の二重で除外していたが、
    -- 在宅時間は生活リズムの指標として有用（在宅の連続日数は歩数より直接的に
    -- 引きこもりを示す）。除外ではなく is_home フラグで区別する。
    WHERE segment_type = 'visit'
    {% if is_incremental() %}
      -- The place_name below is resolved by a LEFT JOIN and physically stored, so a
      -- stay materialized before its place_id reached location_place_cache keeps
      -- 'Unknown Place' forever once it falls out of the reprocess window. Pull those
      -- rows back in regardless of age so they pick up the name on a later run.
      AND (
          event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS DATE)
          OR segment_pk IN (
              SELECT stay_pk FROM {{ this }} WHERE place_name = 'Unknown Place'
          )
      )
    {% endif %}
),

with_coords AS (
    SELECT
        *,
        TRY(CAST(
            regexp_extract(place_lat_lng, '^(-?[\d.]+)°', 1) AS DOUBLE
        )) AS lat,
        TRY(CAST(
            regexp_extract(place_lat_lng, ',\s*(-?[\d.]+)°', 1) AS DOUBLE
        )) AS lng
    FROM visits
    WHERE place_lat_lng IS NOT NULL
),

filtered AS (
    SELECT wc.*,
        -- 自宅判定。座標が半径内、または Google のセマンティック種別が HOME。
        -- 以前はここで自宅を捨てていたため、在宅の記録が一切残らなかった。
        (
            ST_Distance(
                to_spherical_geography(ST_Point(lng, lat)),
                to_spherical_geography(ST_Point({{ home_lng }}, {{ home_lat }}))
            ) <= {{ home_radius_m }}
            OR place_semantic_type LIKE '%HOME%'
        ) AS is_home
    FROM with_coords wc
    WHERE lat IS NOT NULL AND lng IS NOT NULL
),

joined AS (
    SELECT
        f.*,
        c.place_name,
        c.formatted_address
    FROM filtered f
    LEFT JOIN iceberg.life_gold.location_place_cache c ON f.place_id = c.place_id
)

SELECT
    segment_pk AS stay_pk,
    event_date_jst AS stay_date,
    COALESCE(place_name, 'Unknown Place') AS place_name,
    place_id,
    formatted_address,
    lat,
    lng,
    CAST(start_ts_jst AS TIMESTAMP) AS arrived_at,
    CAST(end_ts_jst AS TIMESTAMP) AS departed_at,
    CAST(
        date_diff('minute', CAST(start_ts_jst AS TIMESTAMP), CAST(end_ts_jst AS TIMESTAMP))
        AS INTEGER
    ) AS duration_min,
    -- true = 自宅。UI と AI FB はこれで「外出先だけ」「在宅含む」を選べる。
    is_home,
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM joined
