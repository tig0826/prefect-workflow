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

-- Android のメディア再生イベント（aw-watcher-android-media / media.playback）。
--
-- なぜ必要か: aw_window_events は「アプリが前面にあった」ことしか分からない。
-- こちらは state='playing' の実測 duration を持つので、
--   ・前面にあっただけ / 実際に再生していた の区別
--   ・YouTube Music（作業BGM）と YouTube（動画視聴）の分離
--     → cat_sub='YouTube' に両者が混ざっていると、ブロック対象の評価がずれる
--   ・Udemy の再生量（= 学習の実体）
-- が測れる。
--
-- state の実測分布（2026-08-19〜, n=2311）:
--   playing 1042件 (平均108秒, 最大78分) / stopped 475 / buffering 431 / paused 363
-- playing 以外は状態遷移マーカーで duration がほぼ 0。再生時間を集計するときは
-- 必ず playback_state = 'playing' で絞ること。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_media AS (
    SELECT
        CAST(id AS BIGINT) AS source_event_id,
        'aw_android_media_external' AS source_table,
        dt AS source_dt,
        'pixel-7a-tig' AS hostname,
        CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS event_start_time_jst,
        CAST(duration AS DOUBLE) AS duration_sec,
        element_at(data, 'state') AS playback_state,
        element_at(data, 'app') AS app_name,
        element_at(data, 'package') AS package_name,
        element_at(data, 'title') AS media_title,
        element_at(data, 'artist') AS media_artist,
        element_at(data, 'album') AS media_album
    FROM {{ source('hive_life_bronze', 'aw_android_media_external') }}
    WHERE "timestamp" IS NOT NULL AND id IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),

deduped AS (
    SELECT *,
           -- ★pk と同じ粒度で dedup する★
           -- 以前は (hostname, start, app, title, state) で分けていたため、
           -- 同一 source_event_id + start_ts に複数行が残り pk が重複した
           -- （full-refresh で全履歴を入れたときに unique テストが落ちて発覚）。
           -- pk は (source_table, source_event_id, start_ts) なのでそれに合わせる。
           ROW_NUMBER() OVER (
               PARTITION BY source_table, source_event_id, event_start_time_jst
               ORDER BY duration_sec DESC
           ) AS rn
    FROM raw_media
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
    playback_state,
    app_name,
    package_name,
    -- title/artist/album は本人がダッシュボードで掘るために silver に残す。
    -- AI FB のコンテキストには渡さない（曲名・番組名は分析に不要で、
    -- 中身が LLM に送られる面積を無駄に広げるだけ）。
    media_title,
    media_artist,
    media_album,
    CAST(event_start_time_jst AS DATE) AS event_date_jst,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM deduped
WHERE rn = 1
  AND duration_sec >= 0
