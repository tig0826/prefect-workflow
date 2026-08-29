{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stage_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['day(stage_start_jst)']
) }}

-- Fitbit v1.2 の睡眠段階の**区間時系列**。
--
-- なぜ v1 ではダメか（2026-08-27 実測）:
--   python-fitbit の client.sleep() は v1（classic）を叩き、minuteData（1=asleep/2=restless）
--   しか返さない。段階（deep/light/rem/wake）の区間は v1.2 にしかない。
--   さらに v1 は restless を睡眠に含めるため合計が約20%多い
--   （8/26 の3セッション合計 v1=524分 / v1.2=431分）。
--
-- これで何ができるか:
--   ・覚醒（wake）区間の**時刻**が分かるので、解錠タイムスタンプ（aw_unlock_events）や
--     画面時間（int_aw_media / int_aw_web）と突き合わせられる
--     → 「覚醒した瞬間にスマホを開いているか」は本人が絶対に知り得ない情報
--   ・深睡眠が夜のどこに集中しているかが見える（就寝が遅いと前半の深睡眠が削られる）
--
-- 既存の fitbit_sleep（v1 由来）は互換のため残す。こちらは追加。

{% set reprocess_days = var('reprocess_days', 14) %}

-- 再処理の起点。WHERE 句の中にサブクエリを直接置くと、下流の CROSS JOIN UNNEST と
-- 組み合わさったときに Trino が decorrelate できず
-- 「Given correlated subquery is not supported」で落ちる。
-- CTE にして CROSS JOIN で持ち込めば普通の結合になる。
WITH cutoff AS (
    {% if is_incremental() %}
    -- COALESCE 必須。テーブルが空だと MAX が NULL になり `dt >= NULL` が常に偽で、
    -- 一度空になったら永久に何も入らなくなる（実際に踏んだ）。
    SELECT COALESCE(
        CAST(date_add('day', -{{ reprocess_days }}, CAST(MAX(source_dt) AS DATE)) AS VARCHAR),
        '1970-01-01'
    ) AS min_dt
    FROM {{ this }}
    {% else %}
    SELECT CAST(DATE '1970-01-01' AS VARCHAR) AS min_dt
    {% endif %}
),

raw_fitbit AS (
    SELECT
        f.dt,
        json_extract_scalar(f.raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }} f
    CROSS JOIN cutoff c
    WHERE f.dt >= c.min_dt
),

sessions AS (
    SELECT
        dt,
        CAST(json_extract(real_json, '$.sleep_stages.sleep') AS ARRAY(JSON)) AS sleep_array
    FROM raw_fitbit
    WHERE json_extract(real_json, '$.sleep_stages.sleep') IS NOT NULL
),

unnested_sessions AS (
    SELECT
        dt,
        CAST(json_extract_scalar(s.el, '$.logId') AS BIGINT) AS log_id,
        CAST(json_extract_scalar(s.el, '$.isMainSleep') AS BOOLEAN) AS is_main_sleep,
        json_extract_scalar(s.el, '$.type') AS sleep_type,
        -- levels.data と shortData を結合して1本の区間列にする。
        -- shortData は「短時間覚醒」で data とは別配列になっている。
        CAST(json_extract(s.el, '$.levels.data') AS ARRAY(JSON)) AS levels_data,
        CAST(json_extract(s.el, '$.levels.shortData') AS ARRAY(JSON)) AS short_data
    FROM sessions
    CROSS JOIN UNNEST(sleep_array) AS s(el)
    WHERE json_extract_scalar(s.el, '$.logId') IS NOT NULL
),

main_intervals AS (
    SELECT
        dt, log_id, is_main_sleep, sleep_type,
        false AS is_short_wake,
        CAST(REPLACE(json_extract_scalar(d.el, '$.dateTime'), 'T', ' ') AS TIMESTAMP) AS stage_start_jst,
        json_extract_scalar(d.el, '$.level') AS stage,
        CAST(json_extract_scalar(d.el, '$.seconds') AS INTEGER) AS stage_seconds
    FROM unnested_sessions
    CROSS JOIN UNNEST(levels_data) AS d(el)
),

short_intervals AS (
    SELECT
        dt, log_id, is_main_sleep, sleep_type,
        true AS is_short_wake,
        CAST(REPLACE(json_extract_scalar(d.el, '$.dateTime'), 'T', ' ') AS TIMESTAMP) AS stage_start_jst,
        json_extract_scalar(d.el, '$.level') AS stage,
        CAST(json_extract_scalar(d.el, '$.seconds') AS INTEGER) AS stage_seconds
    FROM unnested_sessions
    CROSS JOIN UNNEST(short_data) AS d(el)
),

combined AS (
    SELECT * FROM main_intervals
    UNION ALL
    SELECT * FROM short_intervals
),

-- 同じパーティションを取り直したときに区間が二重にならないようにする
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY log_id, stage_start_jst, is_short_wake
            ORDER BY stage_seconds DESC
        ) AS rn
    FROM combined
    WHERE stage_start_jst IS NOT NULL AND stage_seconds > 0
)

SELECT
    to_hex(md5(to_utf8(
        CAST(log_id AS VARCHAR) || '|' || to_iso8601(stage_start_jst) || '|' ||
        CAST(is_short_wake AS VARCHAR)
    ))) AS stage_pk,
    dt AS source_dt,
    log_id,
    is_main_sleep,
    sleep_type,
    is_short_wake,
    stage_start_jst,
    stage_start_jst + interval '1' second * stage_seconds AS stage_end_jst,
    stage_seconds,
    stage,
    CAST(stage_start_jst AS DATE) AS stage_date_jst,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM deduped
WHERE rn = 1
