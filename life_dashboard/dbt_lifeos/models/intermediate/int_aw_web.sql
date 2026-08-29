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

-- Android Chrome のタブ滞在を「ドメイン + 分類」に落としたモデル。
--
-- ★プライバシー境界★ このモデルは life_gold スキーマに出るため、
-- url のパス部分と page_title は**意図的に捨てている**。生値が必要なときは
-- life_silver.aw_web_events を見る。
--
-- cat_sub = 'プライベート' について:
--   本人がダッシュボードで内訳を確認できるように、ここでは独立したラベルを残す。
--   ただし AI FB 向けのマート側では 'ネットサーフィン' に畳んで、
--   FB がこのカテゴリを他と区別できないようにする（→ mrt_ai_context_daily）。
--   FB が必要とするのは「深夜にスマホで◯分ブラウジングしていた」という事実だけで、
--   中身の識別は不要。
--
-- 分類の粒度は int_aw_categorized の cat_main / cat_sub と揃えてある
-- （下流で UNION して同じ軸で集計できるようにするため）。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH web AS (
    SELECT
        aw_event_pk, hostname, event_start_time_jst, event_end_time_jst,
        duration_sec, domain, is_audible, event_date_jst
    FROM {{ ref('aw_web_events') }}
    WHERE is_incognito = false
    {% if is_incremental() %}
      AND event_start_time_jst >= date_add('day', -{{ reprocess_days }}, date_trunc('day', current_timestamp AT TIME ZONE 'Asia/Tokyo'))
    {% endif %}
),

classified AS (
    SELECT *,
        CASE
            -- 広告・計測系は本人の行動ではないので除外対象に印を付ける
            WHEN domain LIKE '%googleadservices.com' OR domain LIKE '%doubleclick.net'
              OR domain LIKE '%googlesyndication.com' OR domain LIKE '%google-analytics.com'
              OR domain LIKE '%adservice%' OR domain LIKE '%applovin%'
              OR domain LIKE '%moloco.com' OR domain LIKE '%inmobi.com' THEN 'AD'

            -- 自宅インフラ・自作ダッシュボード
            WHEN domain LIKE '%.mynet' OR domain = 'localhost' THEN 'DEVELOP'

            -- 学習・技術情報
            WHEN domain LIKE '%udemy.com' OR domain LIKE '%qiita.com'
              OR domain LIKE '%zenn.dev' OR domain LIKE '%note.com'
              OR domain LIKE '%stackoverflow.com' OR domain LIKE '%github.com'
              OR domain LIKE '%readthedocs%' OR domain LIKE '%docs.%' THEN 'DEVELOP'

            -- 漫画（AGH のブロック対象と揃える）
            WHEN domain LIKE '%gigaviewer.com' OR domain LIKE '%shonenjumpplus.com'
              OR domain LIKE '%shonenjump.com' OR domain LIKE '%sunday-webry.com'
              OR domain LIKE '%comic-days.com' OR domain LIKE '%manga-one.com'
              OR domain LIKE '%magazinepocket.com' OR domain LIKE '%nicomanga.jp' THEN 'MANGA'

            -- 動画・配信
            WHEN domain LIKE '%youtube.com' OR domain LIKE '%bilibili.com'
              OR domain LIKE '%nicovideo.jp' OR domain LIKE '%unext.jp'
              OR domain LIKE '%indazn.com' OR domain LIKE '%dazn.com'
              OR domain LIKE '%abema%' OR domain LIKE '%twitch.tv'
              OR domain LIKE '%netflix.com' THEN 'MEDIA'

            -- ゲーム情報（ドラクエ10の攻略サイト等）
            WHEN domain LIKE '%dqx.jp' OR domain LIKE '%hoimiso%' THEN 'GAME'

            -- 買い物
            WHEN domain LIKE '%amazon.co.jp' OR domain LIKE '%rakuten.co.jp'
              OR domain LIKE '%mercari%' THEN 'LIFE'

            -- SNS
            WHEN domain LIKE '%x.com' OR domain LIKE '%twitter.com' THEN 'SOCIAL'

            ELSE 'BROWSING'
        END AS cat_main
    FROM web
),

sub AS (
    SELECT *,
        CASE
            WHEN cat_main = 'AD' THEN '広告・計測'
            WHEN cat_main = 'DEVELOP' AND (domain LIKE '%.mynet' OR domain = 'localhost') THEN '個人開発(自宅インフラ)'
            WHEN cat_main = 'DEVELOP' THEN '学習'
            WHEN cat_main = 'MANGA' THEN '漫画(Web)'
            WHEN cat_main = 'MEDIA' THEN '動画(Web)'
            WHEN cat_main = 'GAME' THEN 'ゲーム情報'
            WHEN cat_main = 'LIFE' THEN 'ネットショッピング'
            WHEN cat_main = 'SOCIAL' THEN 'SNS'
            -- 検索は「何かを調べていた」ことの指標として分けておく
            WHEN domain LIKE '%google.com' OR domain LIKE '%bing.com'
              OR domain LIKE '%duckduckgo.com' THEN '検索'
            -- 成人向け・その手のサイトは中身を上位層に出さないための中立ラベル。
            -- ドメイン名の羅列で判定している（網羅は目的ではなく、量が測れれば十分）。
            WHEN domain LIKE '%nightmare-salon.com' OR domain LIKE '%uise-official.com'
              OR domain LIKE '%momon-ga.com' OR domain LIKE '%hitomi.la'
              OR domain LIKE '%candfans.jp' OR domain LIKE '%cityheaven.net'
              OR domain LIKE '%dlsite%' OR domain LIKE '%fanza%' THEN 'プライベート'
            ELSE 'ネットサーフィン'
        END AS cat_sub
    FROM classified
)

SELECT
    aw_event_pk AS event_pk,
    event_date_jst,
    'activitywatch' AS source_system,
    'aw_web' AS source_detail,
    hostname,
    CAST(event_start_time_jst AS TIMESTAMP) AS start_ts,
    CAST(event_end_time_jst AS TIMESTAMP) AS end_ts,
    duration_sec,
    domain,
    is_audible,
    cat_main,
    cat_sub,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM sub
-- 広告・計測ドメインは本人の行動時間ではないので落とす
WHERE cat_main <> 'AD'
