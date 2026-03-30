{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH window_events AS (
    SELECT *
    FROM {{ ref('aw_window_events') }}
    {% if is_incremental() %}
    WHERE event_start_time_jst >= date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo'))
    {% endif %}
),

afk_events AS (
    SELECT *
    FROM {{ ref('aw_afk_events') }}
    {% if is_incremental() %}
    WHERE afk_start_time_jst >= date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo'))
    {% endif %}
),

split_events AS (
    SELECT
        w.aw_event_pk,
        w.hostname,
        w.usage_type,
        w.app_name_raw,
        w.window_title_raw,
        GREATEST(w.event_start_time_jst, COALESCE(a.afk_start_time_jst, w.event_start_time_jst)) AS start_ts,
        LEAST(w.event_end_time_jst, COALESCE(a.afk_end_time_jst, w.event_end_time_jst)) AS end_ts,
        COALESCE(a.afk_status = 'afk', false) AS is_afk
    FROM window_events w
    LEFT JOIN afk_events a
      ON w.hostname = a.hostname
      AND w.event_start_time_jst < a.afk_end_time_jst
      AND w.event_end_time_jst > a.afk_start_time_jst
),

categorized AS (
    SELECT
        to_hex(md5(to_utf8(CAST(aw_event_pk AS varchar) || '|' || to_iso8601(start_ts) || '|' || CAST(is_afk AS varchar)))) AS event_pk,
        CAST(start_ts AS date) AS event_date_jst,
        'activitywatch' AS source_system,
        'aw_window' AS source_detail,
        start_ts,
        end_ts,
        is_afk,
        app_name_raw AS raw_app_name,
        window_title_raw AS raw_window_title,
        usage_type AS raw_usage_type,
        hostname AS raw_hostname,

        CASE
            WHEN usage_type = 'work' THEN 'WORK'
            WHEN LOWER(app_name_raw) LIKE '%ghostty%' OR LOWER(app_name_raw) LIKE '%vscode%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%notion%' OR LOWER(window_title_raw) LIKE '%notion%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%gogh%' OR LOWER(window_title_raw) LIKE '%gogh%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%chatgpt%' OR LOWER(window_title_raw) LIKE '%chatgpt%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%gemini%' OR LOWER(window_title_raw) LIKE '%gemini%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%qiita%' OR LOWER(window_title_raw) LIKE '%qiita%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%kindle%' OR LOWER(window_title_raw) LIKE '%kindle%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%.mynet%' OR LOWER(window_title_raw) LIKE '%.mynet%'
              OR LOWER(app_name_raw) LIKE '%rancher%' OR LOWER(window_title_raw) LIKE '%rancher%'
              OR LOWER(app_name_raw) LIKE '%superset%' OR LOWER(window_title_raw) LIKE '%superset%'
              OR LOWER(app_name_raw) LIKE '%activitywatch%' OR LOWER(window_title_raw) LIKE '%activitywatch%'
              OR LOWER(app_name_raw) LIKE '%prefect%' OR LOWER(window_title_raw) LIKE '%prefect%'
              OR LOWER(app_name_raw) LIKE '%minio%' OR LOWER(window_title_raw) LIKE '%minio%' THEN 'DEVELOP'
            WHEN LOWER(app_name_raw) LIKE '%slack%' OR LOWER(app_name_raw) LIKE '%discord%' THEN 'SOCIAL'
            WHEN LOWER(app_name_raw) LIKE '%x.com%' OR LOWER(window_title_raw) LIKE '%x.com%'
              OR LOWER(window_title_raw) LIKE '%twitter.com%' OR LOWER(window_title_raw) LIKE '% / x %' THEN 'SOCIAL'
            WHEN LOWER(app_name_raw) LIKE '%u-next%' OR LOWER(window_title_raw) LIKE '%u-next%' THEN 'MEDIA'
            WHEN LOWER(app_name_raw) LIKE '%dazn%' OR LOWER(window_title_raw) LIKE '%dazn%' THEN 'MEDIA'
            WHEN LOWER(app_name_raw) LIKE '%youtube%' OR LOWER(window_title_raw) LIKE '%youtube%' THEN 'MEDIA'
            WHEN LOWER(app_name_raw) LIKE '%twitch%' OR LOWER(window_title_raw) LIKE '%twitch%' THEN 'MEDIA'
            WHEN LOWER(app_name_raw) LIKE '%ニコニコ漫画%' OR LOWER(window_title_raw) LIKE '%ニコニコ漫画%'
              OR LOWER(app_name_raw) LIKE '%コミックDAYS%' OR LOWER(window_title_raw) LIKE '%コミックDAYS%'
              OR LOWER(app_name_raw) LIKE '%サンデーうぇぶり%' OR LOWER(window_title_raw) LIKE '%サンデーうぇぶり%'
              OR LOWER(app_name_raw) LIKE '%マンガワン%' OR LOWER(window_title_raw) LIKE '%マンガワン%' THEN 'MANGA'
            WHEN LOWER(app_name_raw) LIKE '%chrome%' OR LOWER(app_name_raw) LIKE '%edge%' OR LOWER(app_name_raw) LIKE '%brave%' THEN 'BROWSING'
            WHEN usage_type = 'gaming' THEN 'GAME'
            WHEN LOWER(window_title_raw) LIKE '%amazon%' OR LOWER(window_title_raw) LIKE '%楽天市場%' THEN 'LIFE'
            WHEN LOWER(app_name_raw) LIKE '%uber eats%' THEN 'LIFE'
            ELSE 'BROWSING'
        END AS cat_main,

        CASE
            WHEN usage_type = 'work' THEN '業務'
            WHEN LOWER(app_name_raw) LIKE '%ghostty%' OR LOWER(app_name_raw) LIKE '%vscode%' THEN '個人開発(コーディング)'
            WHEN LOWER(app_name_raw) LIKE '%notion%' OR LOWER(window_title_raw) LIKE '%notion%' THEN 'notion'
            WHEN LOWER(app_name_raw) LIKE '%gogh%' OR LOWER(window_title_raw) LIKE '%gogh%' THEN 'Gogh'
            WHEN LOWER(app_name_raw) LIKE '%chatgpt%' OR LOWER(window_title_raw) LIKE '%chatgpt%'
              OR LOWER(app_name_raw) LIKE '%gemini%' OR LOWER(window_title_raw) LIKE '%gemini%' THEN '個人開発(AIペアプロ)'
            WHEN LOWER(app_name_raw) LIKE '%.mynet%' OR LOWER(window_title_raw) LIKE '%.mynet%'
              OR LOWER(app_name_raw) LIKE '%rancher%' OR LOWER(window_title_raw) LIKE '%rancher%'
              OR LOWER(app_name_raw) LIKE '%superset%' OR LOWER(window_title_raw) LIKE '%superset%'
              OR LOWER(app_name_raw) LIKE '%activitywatch%' OR LOWER(window_title_raw) LIKE '%activitywatch%'
              OR LOWER(app_name_raw) LIKE '%prefect%' OR LOWER(window_title_raw) LIKE '%prefect%'
              OR LOWER(app_name_raw) LIKE '%minio%' OR LOWER(window_title_raw) LIKE '%minio%' THEN '個人開発(自宅インフラ)'
            WHEN LOWER(app_name_raw) LIKE '%qiita%' OR LOWER(window_title_raw) LIKE '%qiita%' THEN 'qiita'
            WHEN LOWER(app_name_raw) LIKE '%kindle%' OR LOWER(window_title_raw) LIKE '%kindle%' THEN '読書'
            WHEN LOWER(app_name_raw) LIKE '%slack%' OR LOWER(app_name_raw) LIKE '%discord%' THEN 'コミュニティ'
            WHEN LOWER(app_name_raw) LIKE '%x.com%' OR LOWER(window_title_raw) LIKE '%x.com%'
              OR LOWER(window_title_raw) LIKE '%twitter.com%' OR LOWER(window_title_raw) LIKE '% / x %' THEN 'SNS'
            WHEN LOWER(app_name_raw) LIKE '%u-next%' OR LOWER(window_title_raw) LIKE '%u-next%' THEN 'U-NEXT'
            WHEN LOWER(app_name_raw) LIKE '%dazn%' OR LOWER(window_title_raw) LIKE '%dazn%' THEN 'サッカー視聴'
            WHEN LOWER(app_name_raw) LIKE '%youtube%' OR LOWER(window_title_raw) LIKE '%youtube%' THEN 'YouTube'
            WHEN LOWER(app_name_raw) LIKE '%twitch%' OR LOWER(window_title_raw) LIKE '%twitch%' THEN 'Twitch'
            WHEN LOWER(app_name_raw) LIKE '%ニコニコ漫画%' OR LOWER(window_title_raw) LIKE '%ニコニコ漫画%' THEN 'ニコニコ漫画'
            WHEN LOWER(app_name_raw) LIKE '%コミックDAYS%' OR LOWER(window_title_raw) LIKE '%コミックDAYS%' THEN 'コミックDAYS'
            WHEN LOWER(app_name_raw) LIKE '%サンデーうぇぶり%' OR LOWER(window_title_raw) LIKE '%サンデーうぇぶり%' THEN 'サンデーうぇぶり'
            WHEN LOWER(app_name_raw) LIKE '%マンガワン%' OR LOWER(window_title_raw) LIKE '%マンガワン%' THEN 'マンガワン'
            WHEN usage_type = 'gaming' THEN 'ゲーム'
            WHEN LOWER(window_title_raw) LIKE '%amazon%' OR LOWER(window_title_raw) LIKE '%楽天市場%' THEN 'ネットショッピング'
            WHEN LOWER(app_name_raw) LIKE '%uber eats%' THEN 'Uber Eats'
            ELSE 'ネットサーフィン'
        END AS cat_sub
    FROM split_events
)

SELECT
    event_pk,
    event_date_jst,
    source_system,
    source_detail,
    CAST(start_ts AS TIMESTAMP) AS start_ts,
    CAST(end_ts AS TIMESTAMP) AS end_ts,
    is_afk,
    raw_app_name,
    raw_window_title,
    raw_usage_type,
    raw_hostname AS hostname,
    cat_main,
    cat_sub,
    CASE
        WHEN cat_main = 'WORK' THEN 50
        WHEN cat_main = 'DEVELOP' THEN 50
        WHEN cat_main = 'SOCIAL' THEN 40
        WHEN raw_usage_type = 'gaming' AND cat_sub = 'ゲーム' THEN 38
        WHEN cat_main = 'ENTERTAINMENT' THEN 35
        WHEN cat_main = 'LIFE' THEN 30
        ELSE 25
    END AS priority
FROM categorized
