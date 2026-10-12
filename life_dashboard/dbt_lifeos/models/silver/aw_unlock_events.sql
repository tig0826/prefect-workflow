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
    partitioned_by=['day(unlock_ts_jst)']
) }}

-- Android のロック解除イベント（aw-watcher-android-unlock / os.lockscreen.unlocks）。
-- duration は常に 0、data は空なので「解錠した瞬間」のタイムスタンプだけを持つ。
--
-- なぜ必要か: 解錠回数は「注意の断片化」の直接指標で、実測 54回/日。
-- mrt_behavior_slots_15m からは原理的に出てこない（あれは15分スロットの
-- 勝者1カテゴリしか持たないので、瞬間的な行動が消える）。
-- 「昨夜3時台に21回スマホを開いている」のように、本人が自分では数えられない
-- 事実を観測可能な形で示せる。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_unlock AS (
    SELECT
        CAST(id AS BIGINT) AS source_event_id,
        'aw_android_unlock_external' AS source_table,
        dt AS source_dt,
        'pixel-7a-tig' AS hostname,
        CAST(from_iso8601_timestamp(CAST("timestamp" AS VARCHAR)) AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS unlock_ts_jst
    FROM {{ source('hive_life_bronze', 'aw_android_unlock_external') }}
    WHERE "timestamp" IS NOT NULL AND id IS NOT NULL
      {% if is_incremental() %} AND dt >= date_format(date_add('day', -{{ reprocess_days }}, current_date), '%Y-%m-%d') {% endif %}
),

-- 同一パーティションを埋め直した場合に同じ解錠が二重に入らないようにする
deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY hostname, unlock_ts_jst
               ORDER BY source_event_id DESC
           ) AS rn
    FROM raw_unlock
)

SELECT
    to_hex(md5(to_utf8(
        source_table || '|' || hostname || '|' || to_iso8601(unlock_ts_jst)
    ))) AS aw_event_pk,
    source_event_id,
    source_table,
    source_dt,
    hostname,
    unlock_ts_jst,
    CAST(unlock_ts_jst AS DATE) AS event_date_jst,
    hour(unlock_ts_jst) AS unlock_hour_jst,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM deduped
WHERE rn = 1
