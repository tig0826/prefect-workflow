{{ config(materialized='view') }}

WITH unified AS (
    SELECT event_pk, event_date_jst, source_system, source_detail, CAST(start_ts AS TIMESTAMP) AS start_ts, CAST(end_ts AS TIMESTAMP) AS end_ts, is_afk, raw_app_name, raw_window_title, raw_usage_type, cat_main, cat_sub, priority, hostname
    FROM {{ ref('int_aw_categorized') }}
    
    UNION ALL
    
    SELECT event_pk, event_date_jst, source_system, source_detail, CAST(start_ts AS TIMESTAMP) AS start_ts, CAST(end_ts AS TIMESTAMP) AS end_ts, false AS is_afk, raw_app_name, raw_window_title, raw_usage_type, cat_main, cat_sub, priority, CAST(NULL AS VARCHAR) AS hostname
    FROM {{ ref('int_fitbit_sleep') }}
    
    UNION ALL
    
    SELECT event_pk, event_date_jst, source_system, source_detail, CAST(start_ts AS TIMESTAMP) AS start_ts, CAST(end_ts AS TIMESTAMP) AS end_ts, false AS is_afk, raw_app_name, raw_window_title, raw_usage_type, cat_main, cat_sub, priority, CAST(NULL AS VARCHAR) AS hostname
    FROM {{ ref('int_fitbit_activity') }}
    
    UNION ALL
    
    SELECT event_pk, event_date_jst, source_system, source_detail, CAST(start_ts AS TIMESTAMP) AS start_ts, CAST(end_ts AS TIMESTAMP) AS end_ts, false AS is_afk, raw_app_name, raw_window_title, raw_usage_type, cat_main, cat_sub, priority, CAST(NULL AS VARCHAR) AS hostname
    FROM {{ ref('int_fitbit_hr_gaps') }}
    
    UNION ALL
    
    SELECT event_pk, event_date_jst, source_system, source_detail, CAST(start_ts AS TIMESTAMP) AS start_ts, CAST(end_ts AS TIMESTAMP) AS end_ts, false AS is_afk, raw_app_name, raw_window_title, raw_usage_type, cat_main, cat_sub, priority, CAST(NULL AS VARCHAR) AS hostname
    FROM {{ ref('int_owntracks_outing') }}
)

SELECT * FROM unified
