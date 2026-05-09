{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_pk',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
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
        w.raw_app_name,
        w.raw_window_title,
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
        to_hex(md5(to_utf8(
            CAST(aw_event_pk AS varchar) || '|' || 
            to_iso8601(start_ts) || '|' || 
            to_iso8601(end_ts) || '|' || 
            CAST(is_afk AS varchar)
        ))) AS event_pk,
        CAST(start_ts AS date) AS event_date_jst,
        'activitywatch' AS source_system,
        'aw_window' AS source_detail,
        start_ts,
        end_ts,
        is_afk,
        raw_app_name,
        raw_window_title,
        usage_type AS raw_usage_type,
        hostname AS raw_hostname,

        CASE
            WHEN usage_type = 'work' THEN 'WORK'
            WHEN LOWER(raw_app_name) LIKE '%ghostty%' OR LOWER(raw_app_name) LIKE '%vscode%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%notion%' OR LOWER(raw_window_title) LIKE '%notion%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%gogh%' OR LOWER(raw_window_title) LIKE '%gogh%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%chatgpt%' OR LOWER(raw_window_title) LIKE '%chatgpt%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%gemini%' OR LOWER(raw_window_title) LIKE '%gemini%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%qiita%' OR LOWER(raw_window_title) LIKE '%qiita%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%kindle%' OR LOWER(raw_window_title) LIKE '%kindle%' THEN 'READING'
            WHEN LOWER(raw_app_name) LIKE '%.mynet%' OR LOWER(raw_window_title) LIKE '%.mynet%'
              OR LOWER(raw_app_name) LIKE '%rancher%' OR LOWER(raw_window_title) LIKE '%rancher%'
              OR LOWER(raw_app_name) LIKE '%superset%' OR LOWER(raw_window_title) LIKE '%superset%'
              OR LOWER(raw_app_name) LIKE '%activitywatch%' OR LOWER(raw_window_title) LIKE '%activitywatch%'
              OR LOWER(raw_app_name) LIKE '%prefect%' OR LOWER(raw_window_title) LIKE '%prefect%'
              OR LOWER(raw_app_name) LIKE '%minio%' OR LOWER(raw_window_title) LIKE '%minio%'
              OR LOWER(raw_app_name) LIKE '%localhost%' OR LOWER(raw_window_title) LIKE '%localhost%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%slack%' OR LOWER(raw_app_name) LIKE '%discord%' THEN 'SOCIAL'
            WHEN LOWER(raw_app_name) LIKE '%x.com%' OR LOWER(raw_window_title) LIKE '%x.com%'
              OR LOWER(raw_window_title) LIKE '%twitter.com%' OR LOWER(raw_window_title) LIKE '% / x %' THEN 'SOCIAL'
            WHEN LOWER(raw_app_name) LIKE '%u-next%' OR LOWER(raw_window_title) LIKE '%u-next%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%dazn%' OR LOWER(raw_window_title) LIKE '%dazn%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%youtube%' OR LOWER(raw_window_title) LIKE '%youtube%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%twitch%' OR LOWER(raw_window_title) LIKE '%twitch%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ漫画%' OR LOWER(raw_window_title) LIKE '%ニコニコ漫画%'
              OR LOWER(raw_app_name) LIKE '%コミックDAYS%' OR LOWER(raw_window_title) LIKE '%コミックDAYS%'
              OR LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' OR LOWER(raw_window_title) LIKE '%サンデーうぇぶり%'
              OR LOWER(raw_app_name) LIKE '%マンガワン%' OR LOWER(raw_window_title) LIKE '%マンガワン%'
              OR LOWER(raw_app_name) LIKE '%ヤンジャン%' OR LOWER(raw_window_title) LIKE '%ヤンジャン%' THEN 'MANGA'
            WHEN LOWER(raw_app_name) LIKE '%chrome%' OR LOWER(raw_app_name) LIKE '%edge%' OR LOWER(raw_app_name) LIKE '%brave%' THEN 'BROWSING'
            WHEN usage_type = 'gaming' THEN 'GAME'
            WHEN LOWER(raw_window_title) LIKE '%amazon%' OR LOWER(raw_window_title) LIKE '%楽天市場%' THEN 'LIFE'
            WHEN LOWER(raw_app_name) LIKE '%uber eats%' THEN 'LIFE'
            ELSE 'BROWSING'
        END AS cat_main,

        CASE
            WHEN usage_type = 'work' THEN '業務'
            WHEN LOWER(raw_app_name) LIKE '%ghostty%' OR LOWER(raw_app_name) LIKE '%vscode%' THEN '個人開発(コーディング)'
            WHEN LOWER(raw_app_name) LIKE '%notion%' OR LOWER(raw_window_title) LIKE '%notion%' THEN 'notion'
            WHEN LOWER(raw_app_name) LIKE '%gogh%' OR LOWER(raw_window_title) LIKE '%gogh%' THEN 'Gogh'
            WHEN LOWER(raw_app_name) LIKE '%chatgpt%' OR LOWER(raw_window_title) LIKE '%chatgpt%'
              OR LOWER(raw_app_name) LIKE '%gemini%' OR LOWER(raw_window_title) LIKE '%gemini%' THEN '個人開発(AIペアプロ)'
            WHEN LOWER(raw_app_name) LIKE '%.mynet%' OR LOWER(raw_window_title) LIKE '%.mynet%'
              OR LOWER(raw_app_name) LIKE '%rancher%' OR LOWER(raw_window_title) LIKE '%rancher%'
              OR LOWER(raw_app_name) LIKE '%superset%' OR LOWER(raw_window_title) LIKE '%superset%'
              OR LOWER(raw_app_name) LIKE '%activitywatch%' OR LOWER(raw_window_title) LIKE '%activitywatch%'
              OR LOWER(raw_app_name) LIKE '%prefect%' OR LOWER(raw_window_title) LIKE '%prefect%'
              OR LOWER(raw_app_name) LIKE '%minio%' OR LOWER(raw_window_title) LIKE '%minio%'
              OR LOWER(raw_app_name) LIKE '%localhost%' OR LOWER(raw_window_title) LIKE '%localhost%' THEN '個人開発(自宅インフラ)'
            WHEN LOWER(raw_app_name) LIKE '%qiita%' OR LOWER(raw_window_title) LIKE '%qiita%' THEN 'qiita'
            WHEN LOWER(raw_app_name) LIKE '%kindle%' OR LOWER(raw_window_title) LIKE '%kindle%' THEN 'Kindle'
            WHEN LOWER(raw_app_name) LIKE '%slack%' OR LOWER(raw_app_name) LIKE '%discord%' THEN 'コミュニティ'
            WHEN LOWER(raw_app_name) LIKE '%x.com%' OR LOWER(raw_window_title) LIKE '%x.com%'
              OR LOWER(raw_window_title) LIKE '%twitter.com%' OR LOWER(raw_window_title) LIKE '% / x %' THEN 'SNS'
            WHEN LOWER(raw_app_name) LIKE '%u-next%' OR LOWER(raw_window_title) LIKE '%u-next%' THEN 'U-NEXT'
            WHEN LOWER(raw_app_name) LIKE '%dazn%' OR LOWER(raw_window_title) LIKE '%dazn%' THEN 'サッカー視聴'
            WHEN LOWER(raw_app_name) LIKE '%youtube%' OR LOWER(raw_window_title) LIKE '%youtube%' THEN 'YouTube'
            WHEN LOWER(raw_app_name) LIKE '%twitch%' OR LOWER(raw_window_title) LIKE '%twitch%' THEN 'Twitch'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ漫画%' OR LOWER(raw_window_title) LIKE '%ニコニコ漫画%' THEN 'ニコニコ漫画'
            WHEN LOWER(raw_app_name) LIKE '%コミックDAYS%' OR LOWER(raw_window_title) LIKE '%コミックDAYS%' THEN 'コミックDAYS'
            WHEN LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' OR LOWER(raw_window_title) LIKE '%サンデーうぇぶり%' THEN 'サンデーうぇぶり'
            WHEN LOWER(raw_app_name) LIKE '%マンガワン%' OR LOWER(raw_window_title) LIKE '%マンガワン%' THEN 'マンガワン'
            WHEN LOWER(raw_app_name) LIKE '%ヤンジャン%' OR LOWER(raw_window_title) LIKE '%ヤンジャン%' THEN 'ヤンジャン＋'
            WHEN usage_type = 'gaming' THEN 'ゲーム'
            WHEN LOWER(raw_window_title) LIKE '%amazon%' OR LOWER(raw_window_title) LIKE '%楽天市場%' THEN 'ネットショッピング'
            WHEN LOWER(raw_app_name) LIKE '%uber eats%' THEN 'Uber Eats'
            ELSE 'ネットサーフィン'
        END AS cat_sub
    FROM split_events
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY event_pk ORDER BY start_ts) AS rn
    FROM categorized
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
FROM deduplicated
WHERE rn = 1
