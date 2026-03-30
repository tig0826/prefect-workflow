{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='owntracks_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['day(event_time_jst)']
) }}

WITH extracted AS (
    SELECT
        json_extract_scalar(raw_json, '$._id') AS source_event_id,
        json_extract_scalar(raw_json, '$._type') AS event_type,
        json_extract_scalar(raw_json, '$.tid') AS tracker_id,
        
        CAST(json_extract_scalar(raw_json, '$.lat') AS DOUBLE) AS latitude,
        CAST(json_extract_scalar(raw_json, '$.lon') AS DOUBLE) AS longitude,
        CAST(json_extract_scalar(raw_json, '$.batt') AS BIGINT) AS battery_level,
        CAST(json_extract_scalar(raw_json, '$.vel') AS BIGINT) AS velocity,
        COALESCE(json_extract_scalar(raw_json, '$.t'), 'move') AS trigger_type,
        
        from_unixtime(CAST(json_extract_scalar(raw_json, '$.tst') AS DOUBLE)) AT TIME ZONE 'Asia/Tokyo' AS event_time_jst_tz,
        
        dt AS source_dt,
        'owntracks_external' AS source_table
    FROM {{ source('hive_life_bronze', 'owntracks_external') }}
    WHERE 
        raw_json IS NOT NULL
        AND json_extract_scalar(raw_json, '$._type') = 'location'
        {% if is_incremental() %}
        AND dt >= date_format(date_add('day', -3, current_date), '%Y-%m-%d')
        {% endif %}
),

deduplicated AS (
    SELECT 
        *,
        CAST(event_time_jst_tz AS TIMESTAMP) AS event_time_jst,
        ROW_NUMBER() OVER (
            PARTITION BY event_time_jst_tz 
            ORDER BY battery_level DESC, source_event_id
        ) AS rn
    FROM extracted
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)

SELECT
    to_hex(md5(to_utf8(
        source_table || '|' || 
        to_iso8601(event_time_jst) || '|' || 
        CAST(latitude AS VARCHAR) || '|' || 
        CAST(longitude AS VARCHAR)
    ))) AS owntracks_pk,

    source_event_id,
    source_table,
    source_dt,

    tracker_id,
    event_time_jst,
    latitude,
    longitude,
    trigger_type,
    
    CASE trigger_type
        WHEN 'p' THEN 'ping'
        WHEN 'u' THEN 'manual'
        WHEN 'move' THEN 'move'
        ELSE trigger_type
    END AS movement_status,
    
    velocity,
    battery_level,

    current_timestamp AT TIME ZONE 'Asia/Tokyo' AS transformed_at_jst
FROM deduplicated
WHERE rn = 1
