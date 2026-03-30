{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='aw_event_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['day(event_start_time_jst)']
) }}

-- 🌟 修正1: 境界バグを防ぐため、Int層と同じ reprocess_days (デフォルト14日) を使う
{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_mac_personal AS (
    SELECT CAST(id AS BIGINT) AS source_event_id, 'aw_window_tignomacbook_pro_external' AS source_table, dt AS source_dt,
           'tignomacbook-pro' AS hostname, 'm5_macbook_pro' AS device_model, 'personal' AS usage_type, 'macOS' AS os_type,
           CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
           CAST(duration AS DOUBLE) AS duration_sec, element_at(data, 'app') AS app_name_raw, element_at(data, 'title') AS window_title_raw, CAST(NULL AS VARCHAR) AS activity_class_raw
    FROM {{ source('hive_life_bronze', 'aw_window_tignomacbook_pro_external') }}
    WHERE "timestamp" IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),
raw_mac_work AS (
    SELECT CAST(id AS BIGINT) AS source_event_id, 'aw_window_a1002995_external' AS source_table, dt AS source_dt,
           'a1002995' AS hostname, 'm1_macbook_pro' AS device_model, 'work' AS usage_type, 'mac' AS os_type,
           CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
           CAST(duration AS DOUBLE) AS duration_sec, element_at(data, 'app') AS app_name_raw, element_at(data, 'title') AS window_title_raw, CAST(NULL AS VARCHAR) AS activity_class_raw
    FROM {{ source('hive_life_bronze', 'aw_window_a1002995_external') }}
    WHERE "timestamp" IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),
raw_windows_gaming AS (
    SELECT CAST(id AS BIGINT) AS source_event_id, 'aw_window_desktop_o8tfag0_external' AS source_table, dt AS source_dt,
           'DESKTOP-O8TFAG0' AS hostname, 'windows_pc' AS device_model, 'gaming' AS usage_type, 'Windows' AS os_type,
           CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
           CAST(duration AS DOUBLE) AS duration_sec, element_at(data, 'app') AS app_name_raw, element_at(data, 'title') AS window_title_raw, CAST(NULL AS VARCHAR) AS activity_class_raw
    FROM {{ source('hive_life_bronze', 'aw_window_desktop_o8tfag0_external') }}
    WHERE "timestamp" IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),
raw_android AS (
    SELECT CAST(id AS BIGINT) AS source_event_id, 'aw_android_test_external' AS source_table, dt AS source_dt,
           'pixel-7a-tig' AS hostname, 'pixel_7a' AS device_model, 'personal' AS usage_type, 'android' AS os_type,
           CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
           CAST(duration AS DOUBLE) AS duration_sec, element_at(data, 'app') AS app_name_raw, CAST(NULL AS VARCHAR) AS window_title_raw, element_at(data, 'classname') AS activity_class_raw
    FROM {{ source('hive_life_bronze', 'aw_android_test_external') }}
    WHERE "timestamp" IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),
merged_raw AS (
    SELECT * FROM raw_mac_personal UNION ALL
    SELECT * FROM raw_mac_work UNION ALL
    SELECT * FROM raw_windows_gaming UNION ALL
    SELECT * FROM raw_android
),

deduped AS (
    SELECT *,
           event_start_time_jst + interval '1' second * duration_sec AS event_end_time_jst,
           -- 🌟 修正2: アプリ名とタイトルまで含めて「本当に全く同じログ」だけを排除する！
           ROW_NUMBER() OVER (
               PARTITION BY hostname, event_start_time_jst, COALESCE(app_name_raw, ''), COALESCE(window_title_raw, '')
               ORDER BY duration_sec DESC, source_event_id DESC
           ) as rn
    FROM merged_raw
    WHERE duration_sec > 0
),

ordered_overlaps AS (
    SELECT *,
           MAX(event_end_time_jst) OVER (
               PARTITION BY hostname 
               ORDER BY event_start_time_jst, event_end_time_jst DESC 
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) as max_prev_end_time
    FROM deduped
    WHERE rn = 1
),

cleansed AS (
    SELECT
        source_event_id, source_table, source_dt, hostname, device_model, usage_type, os_type,
        GREATEST(event_start_time_jst, COALESCE(max_prev_end_time, event_start_time_jst)) AS clean_start_time_jst,
        event_end_time_jst AS clean_end_time_jst,
        app_name_raw, window_title_raw, activity_class_raw
    FROM ordered_overlaps
    WHERE max_prev_end_time IS NULL OR event_end_time_jst > max_prev_end_time
)

SELECT
    to_hex(md5(to_utf8(
        source_table || '|' || CAST(source_event_id AS VARCHAR) || '|' || to_iso8601(clean_start_time_jst)
    ))) AS aw_event_pk,
    source_event_id, source_table, source_dt, hostname, device_model, usage_type, os_type,
    clean_start_time_jst AS event_start_time_jst,
    clean_end_time_jst AS event_end_time_jst,
    date_diff('second', clean_start_time_jst, clean_end_time_jst) AS duration_sec,
    app_name_raw, window_title_raw, activity_class_raw,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM cleansed
WHERE clean_start_time_jst < clean_end_time_jst
  AND app_name_raw NOT IN ('Pixel Launcher', 'loginwindow', 'LockApp.exe', 'LogonUI.exe', 'SearchHost.exe')
