{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='slot_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['activity_date_jst']
) }}

-- AI フィードバック専用の活動マート。
--
-- なぜ mrt_behavior_slots_15m ではダメなのか:
--   あちらは「1スロット = 1カテゴリ」で、priority の勝者だけを残す
--   （実測: 8日 = 768スロット、すべて cat_main が1種類）。
--   人がタイムラインをぱっと見る用途には正しいが、分析には次の情報が消える:
--     ・SLEEP が MEDIA を上書きするので「寝落ち視聴」が見えない
--     ・cat_main 集計なので「MEDIA 60分」までしか分からず
--       YouTube か U-NEXT か漫画かが区別できない
--   このマートは **priority による抑制をしない**。同じ時間に複数の行が立つのが正しい。
--
-- 設計:
--   ・long format（日 × 時 × ソース × ホスト × cat_main × cat_sub → 秒数）
--   ・時をまたぐイベントは境界で正しく分割する（開始時刻に丸めない）
--   ・日付は「その時間バケットの日付」で決まるので、日跨ぎイベントも正しく分かれる
--   ・恣意的な duration 上限は設けない。NOT is_afk で十分
--     （実測 max 143分 / p99.9 40分。is_afk=true 側は max 17時間で、
--       これが「開いたまま放置したウィンドウ」）
--
-- 注意: source をまたぐと時間は重複する（macOS で開発しながらスマホで再生など）。
--   実測で active 合計は 19.3h/日 になる。これは異常値ではなくマルチデバイスの重なり。
--   「1日のうち何分か」を出したいときは source / hostname を絞るか、分単位で集合を取る。
--
-- ★プライバシー★ cat_sub='プライベート'（int_aw_web の成人向けラベル）は
--   ここで 'ネットサーフィン' に畳む。AI FB がこのカテゴリを他と区別できないようにする。
--   FB に必要なのは「深夜にスマホで◯分ブラウジングしていた」という事実だけ。
--   本人が内訳を見たいときは life_gold.int_aw_web / life_silver.aw_web_events を直接見る。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH bounds AS (
    SELECT
        CAST(date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_start_local,
        CAST(date_add('day', 1, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo')) AS timestamp) AS window_end_local
),

events AS (
    -- 1) 前面にあったアプリ／ウィンドウ
    SELECT
        'window' AS source,
        hostname,
        cat_main,
        cat_sub,
        start_ts,
        end_ts
    FROM {{ ref('int_aw_categorized') }}
    WHERE NOT is_afk

    UNION ALL

    -- 2) 実際に再生されていたもの（前面かどうかとは独立）
    SELECT
        'media' AS source,
        hostname,
        cat_main,
        cat_sub,
        start_ts,
        end_ts
    FROM {{ ref('int_aw_media') }}

    UNION ALL

    -- 3) スマホのブラウザで実際に見ていたドメインの分類
    SELECT
        'web' AS source,
        hostname,
        cat_main,
        CASE WHEN cat_sub = 'プライベート' THEN 'ネットサーフィン' ELSE cat_sub END AS cat_sub,
        start_ts,
        end_ts
    FROM {{ ref('int_aw_web') }}

    UNION ALL

    -- 4) 睡眠。独立ソースとして持つことで、同じ時間帯の MEDIA と共存できる
    --    （= 寝落ち視聴や、昼寝と娯楽の重なりが見える）
    SELECT
        'sleep' AS source,
        'fitbit' AS hostname,
        cat_main,
        cat_sub,
        start_ts,
        end_ts
    FROM {{ ref('int_fitbit_sleep') }}
),

scoped AS (
    SELECT e.*
    FROM events e
    WHERE e.end_ts > e.start_ts
    {% if is_incremental() %}
      AND e.start_ts < (SELECT window_end_local FROM bounds)
      AND e.end_ts > (SELECT window_start_local FROM bounds)
    {% endif %}
),

-- イベントを1時間バケットに分割する。
-- end_ts が丁度時境界のとき（例 09:30〜10:00）は 10時台のバケットも生成されるが、
-- 重なりが GREATEST/LEAST で 0 秒になり最後の WHERE seconds > 0 で落ちるので問題ない。
-- （Trino に INTERVAL '1' MILLISECOND は無いため、引き算で潰す実装にはしない）
hour_split AS (
    SELECT
        s.source,
        s.hostname,
        s.cat_main,
        s.cat_sub,
        s.start_ts,
        s.end_ts,
        h AS hour_start
    FROM scoped s
    CROSS JOIN UNNEST(sequence(
        date_trunc('hour', s.start_ts),
        date_trunc('hour', s.end_ts),
        INTERVAL '1' HOUR
    )) AS t(h)
),

aggregated AS (
    SELECT
        CAST(hour_start AS DATE) AS activity_date_jst,
        hour(hour_start) AS hour_jst,
        source,
        hostname,
        cat_main,
        cat_sub,
        SUM(date_diff(
            'second',
            GREATEST(start_ts, hour_start),
            LEAST(end_ts, hour_start + INTERVAL '1' HOUR)
        )) AS seconds,
        COUNT(*) AS event_count
    FROM hour_split
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    to_hex(md5(to_utf8(
        CAST(activity_date_jst AS VARCHAR) || '|' || CAST(hour_jst AS VARCHAR) || '|' ||
        source || '|' || hostname || '|' || cat_main || '|' || cat_sub
    ))) AS slot_pk,
    activity_date_jst,
    hour_jst,
    source,
    hostname,
    cat_main,
    cat_sub,
    seconds,
    ROUND(seconds / 60.0, 1) AS minutes,
    event_count,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM aggregated
WHERE seconds > 0
