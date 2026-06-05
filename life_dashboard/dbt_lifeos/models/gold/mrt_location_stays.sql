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
    WHERE segment_type = 'visit'
      AND (place_semantic_type IS NULL OR place_semantic_type NOT LIKE '%HOME%')
    {% if is_incremental() %}
      AND event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, current_date) AS DATE)
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
    SELECT wc.*
    FROM with_coords wc
    WHERE lat IS NOT NULL AND lng IS NOT NULL
      AND ST_Distance(
            to_spherical_geography(ST_Point(lng, lat)),
            to_spherical_geography(ST_Point({{ home_lng }}, {{ home_lat }}))
          ) > {{ home_radius_m }}
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
    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM joined
