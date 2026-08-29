-- ★merge ではなく delete+insert★
-- AW は**進行中のイベントをポーリングごとに新しい id で返す**。実測（2026-08-20 の U-NEXT）:
--   id 266419(1120秒) → 266460(2020) → 266471(2919) → 266475(3819) → 266484(4719) → 266505(5619)
-- 開始時刻は同じで duration が15分ずつ伸びており、15分間隔のスクレイプ回数と一致する。
-- pk に source_event_id を含むため merge では毎回「別の行」として挿入され、
-- **消えた古いスナップショットが削除されずに残る**。
-- 結果、同じ時間帯が何重にも計上されていた（8日間で重なり 5,016分・515ペア）。
-- bronze は日次スナップショット（上書き）なので、対象日の行をまとめて入れ替えるのが正しい。
--
-- 注意: config() は Jinja 式なので中に -- コメントは書けない。説明はここに置く。
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_dt',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['day(event_start_time_jst)']
) }}

-- Android Chrome のタブ滞在イベント（aw-watcher-android-web-chrome / web.tab.current）。
--
-- なぜ必要か: 現状 BROWSING が 1.9h/日 あるのに中身が完全に不明で、
-- 「ネットサーフィン」という1つの箱に落ちている。ここを開けないと
-- 「深夜のスマホ閲覧」が学習なのか娯楽なのか区別できない。
--
-- ★プライバシー境界★
-- url / page_title の生値は **この silver モデルにだけ**置く。
-- silver は life_silver スキーマ、intermediate 以降は life_gold スキーマなので、
-- 上位層（int_aw_web）はドメイン単位＋分類だけを持ち、パスとタイトルを捨てる。
-- AI FB のコンテキストビルダーは url / page_title を絶対に SELECT しないこと。
-- 本人がダッシュボードやチャットから掘りたいときは life_silver を直接見る。
--
-- データ形式の注意: url にスキームは付かない（"life.mynet" や
-- "cityheaven.net/saitama/A1102/...?shopmenu=2#tabMenu" の形）。
-- したがってドメイン抽出は "最初の / より前" を取るだけでよい。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_web AS (
    SELECT
        CAST(id AS BIGINT) AS source_event_id,
        'aw_android_web_chrome_external' AS source_table,
        dt AS source_dt,
        'pixel-7a-tig' AS hostname,
        CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
        CAST(duration AS DOUBLE) AS duration_sec,
        element_at(data, 'url') AS url,
        element_at(data, 'title') AS page_title,
        element_at(data, 'browser') AS browser_package,
        element_at(data, 'audible') = 'true' AS is_audible,
        element_at(data, 'incognito') = 'true' AS is_incognito
    FROM {{ source('hive_life_bronze', 'aw_android_web_chrome_external') }}
    WHERE "timestamp" IS NOT NULL AND id IS NOT NULL
      AND element_at(data, 'url') IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),

with_domain AS (
    SELECT *,
        -- スキームが無いので最初の '/' より前がホスト。ポートとwww.は落とす。
        REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(SPLIT_PART(url, '/', 1)), ':[0-9]+$', ''),
            '^www\.', ''
        ) AS domain
    FROM raw_web
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY hostname, event_start_time_jst, url
               ORDER BY duration_sec DESC, source_event_id DESC
           ) AS rn
    FROM with_domain
)

SELECT
    to_hex(md5(to_utf8(
        source_table || '|' || CAST(source_event_id AS VARCHAR) || '|' || to_iso8601(event_start_time_jst)
    ))) AS aw_event_pk,
    source_event_id,
    source_table,
    source_dt,
    hostname,
    event_start_time_jst,
    event_start_time_jst + interval '1' second * duration_sec AS event_end_time_jst,
    duration_sec,
    domain,
    url,
    page_title,
    browser_package,
    is_audible,
    is_incognito,
    CAST(event_start_time_jst AS DATE) AS event_date_jst,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM deduped
WHERE rn = 1
  AND duration_sec > 0
  AND domain <> ''
