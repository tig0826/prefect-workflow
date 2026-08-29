-- ★merge ではなく delete+insert★
-- event_pk に start_ts / end_ts / is_afk を含むため、上流の境界が変わると
-- 「別の行」として挿入され古い行が残る。上流 aw_window_events が
-- AW のポーリングごとに新 id を振られる問題（同モデルのコメント参照）を
-- そのまま継承して二重計上になっていた。対象日をまとめて入れ替える。
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date_jst',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

-- 実際に再生されていた時間（state='playing' のみ）をアプリ単位で持つモデル。
--
-- int_aw_categorized（= 前面にあったアプリ）との違いが本質的:
--   前面 = 見ていたかもしれない / playing = 実際に音や映像が出ていた
-- 例えば YouTube を開いたまま放置していた時間は前面には出るが playing には出ない。
-- 逆に画面を消して音楽を聴いていた時間は playing にだけ出る。
--
-- cat_sub は int_aw_categorized と揃えるが、1点だけ意図的に細かくしている:
--   YouTube Music を 'YouTube Music' として YouTube から分離する。
--   int_aw_categorized は両方 'YouTube' に落としているため、
--   「作業BGM」と「動画視聴」が混ざってブロック対象の評価がずれる。
--   実測（2026-08-19〜）でも YouTube Music 332件 / YouTube 241件で無視できない量。
--
-- title/artist/album は silver に残してあり、ここには持ち上げない
-- （曲名・番組名は分析に不要で、LLM に送る面積を広げるだけ）。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH playing AS (
    SELECT
        aw_event_pk, hostname, event_start_time_jst, event_end_time_jst,
        duration_sec, app_name, package_name, event_date_jst
    FROM {{ ref('aw_media_events') }}
    WHERE playback_state = 'playing'
      AND duration_sec > 0
    {% if is_incremental() %}
      AND event_start_time_jst >= date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo'))
    {% endif %}
),

classified AS (
    SELECT *,
        CASE
            WHEN LOWER(app_name) LIKE '%udemy%' THEN 'DEVELOP'
            WHEN LOWER(app_name) LIKE '%youtube music%'
              OR LOWER(package_name) LIKE '%youtube.music%'
              OR LOWER(app_name) LIKE '%amazon music%'
              OR LOWER(app_name) LIKE '%spotify%' THEN 'MUSIC'
            ELSE 'MEDIA'
        END AS cat_main,
        CASE
            WHEN LOWER(app_name) LIKE '%udemy%' THEN '学習'
            WHEN LOWER(app_name) LIKE '%youtube music%'
              OR LOWER(package_name) LIKE '%youtube.music%' THEN 'YouTube Music'
            WHEN LOWER(app_name) LIKE '%amazon music%' THEN 'Amazon Music'
            WHEN LOWER(app_name) LIKE '%spotify%' THEN 'Spotify'
            WHEN LOWER(app_name) LIKE '%youtube%' THEN 'YouTube'
            WHEN LOWER(app_name) LIKE '%u-next%' THEN 'U-NEXT'
            WHEN LOWER(app_name) LIKE '%ニコニコ%' THEN 'niconico'
            WHEN LOWER(app_name) LIKE '%twitch%' THEN 'Twitch'
            WHEN LOWER(app_name) LIKE '%abema%' THEN 'ABEMA'
            WHEN LOWER(app_name) LIKE '%dazn%' THEN 'スポーツ観戦'
            WHEN LOWER(app_name) LIKE '%firefox%' OR LOWER(app_name) LIKE '%chrome%' THEN 'ブラウザ再生'
            ELSE COALESCE(app_name, '不明')
        END AS cat_sub
    FROM playing
)

SELECT
    aw_event_pk AS event_pk,
    event_date_jst,
    'activitywatch' AS source_system,
    'aw_media' AS source_detail,
    hostname,
    CAST(event_start_time_jst AS TIMESTAMP) AS start_ts,
    CAST(event_end_time_jst AS TIMESTAMP) AS end_ts,
    duration_sec,
    app_name,
    package_name,
    cat_main,
    cat_sub,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM classified
