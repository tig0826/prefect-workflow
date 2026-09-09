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
            WHEN LOWER(raw_app_name) LIKE '%udemy%' OR LOWER(raw_window_title) LIKE '%udemy%' THEN 'DEVELOP'
            -- 資格試験の過去問道場（*-siken.com）。ブラウザで開くため
            -- 総称の chrome→BROWSING ルールより前に判定する必要がある。
            -- ドメインが取れるのは Chrome 拡張だけで、PC のウィンドウタイトルには
            -- 「応用情報技術者試験過去問道場 第10問｜応用情報技術者試験.com」の形で
            -- 入るので両方拾う（実測 954件 / 375分がネットサーフィンに落ちていた）。
            WHEN LOWER(raw_window_title) LIKE '%siken.com%'
              OR raw_window_title LIKE '%過去問道場%'
              OR raw_window_title LIKE '%技術者試験.com%' THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%kindle%' OR LOWER(raw_window_title) LIKE '%kindle%' THEN 'READING'
            -- ★.mynet の総称ルールより前に置くこと★
            -- 自作の Life Dashboard は開発対象ではなく既に実用しているツールなので、
            -- 個人開発ではなく生活管理として扱う。タイトルに .mynet が入る経路もあるため
            -- 下の DEVELOP ルールより先に判定する必要がある。
            WHEN LOWER(raw_window_title) LIKE '%life dashboard%'
              OR LOWER(raw_app_name) LIKE '%life dashboard%' THEN 'LIFE'
            WHEN LOWER(raw_app_name) LIKE '%.mynet%' OR LOWER(raw_window_title) LIKE '%.mynet%'
              OR LOWER(raw_app_name) LIKE '%rancher%' OR LOWER(raw_window_title) LIKE '%rancher%'
              OR LOWER(raw_app_name) LIKE '%superset%' OR LOWER(raw_window_title) LIKE '%superset%'
              OR LOWER(raw_app_name) LIKE '%activitywatch%' OR LOWER(raw_window_title) LIKE '%activitywatch%'
              OR LOWER(raw_app_name) LIKE '%prefect%' OR LOWER(raw_window_title) LIKE '%prefect%'
              OR LOWER(raw_app_name) LIKE '%minio%' OR LOWER(raw_window_title) LIKE '%minio%'
              OR LOWER(raw_app_name) LIKE '%localhost%' OR LOWER(raw_window_title) LIKE '%localhost%'
              THEN 'DEVELOP'
            WHEN LOWER(raw_app_name) LIKE '%slack%' OR LOWER(raw_app_name) LIKE '%discord%' THEN 'SOCIAL'
            WHEN LOWER(raw_app_name) LIKE '%x.com%' OR LOWER(raw_window_title) LIKE '%x.com%'
              OR LOWER(raw_window_title) LIKE '%twitter.com%' OR LOWER(raw_window_title) LIKE '% / x %' THEN 'SOCIAL'
            -- ★音楽は娯楽と分ける★
            -- YouTube Music は前面時間では 'YouTube' → MEDIA に落ちていたため、
            -- ダッシュボードの Leisure（ENT_CATS = MEDIA/MANGA/GAME/SOCIAL）に
            -- 作業BGMが混ざっていた。実測で YouTube Music は再生時間で最大
            -- （754分/7日）で、生活を乱す娯楽とは性質が違う。
            -- cat_main='MUSIC' は ENT_CATS に含まれないので自動的に娯楽から外れる。
            -- int_aw_media 側も既に MUSIC を使っているので表記を揃える。
            -- **YouTube より先に判定すること**（YouTube Music が YouTube にマッチするため）。
            WHEN LOWER(raw_app_name) LIKE '%youtube music%' OR LOWER(raw_window_title) LIKE '%youtube music%'
              OR LOWER(raw_app_name) LIKE '%amazon music%' OR LOWER(raw_window_title) LIKE '%amazon music%'
              OR LOWER(raw_app_name) LIKE '%spotify%' OR LOWER(raw_window_title) LIKE '%spotify%'
              OR LOWER(raw_app_name) LIKE '%apple music%' THEN 'MUSIC'
            WHEN LOWER(raw_app_name) LIKE '%u-next%' OR LOWER(raw_window_title) LIKE '%u-next%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%dazn%' OR LOWER(raw_window_title) LIKE '%dazn%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%youtube%' OR LOWER(raw_window_title) LIKE '%youtube%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ動画%' OR LOWER(raw_app_name) LIKE '%ニコニコ生放送%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%twitch%' OR LOWER(raw_window_title) LIKE '%twitch%' THEN 'MEDIA'
            -- 2026-08-29 追加。'ネットサーフィン' に落ちていた動画サービス。
            WHEN LOWER(raw_app_name) LIKE '%prime video%' OR LOWER(raw_window_title) LIKE '%prime video%'
              OR LOWER(raw_app_name) LIKE '%abema%' OR LOWER(raw_window_title) LIKE '%abema%'
              OR LOWER(raw_app_name) LIKE '%fotmob%' THEN 'MEDIA'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ漫画%' OR LOWER(raw_window_title) LIKE '%ニコニコ漫画%'
              OR LOWER(raw_app_name) LIKE '%コミックdays%' OR LOWER(raw_window_title) LIKE '%コミックdays%'
              OR LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' OR LOWER(raw_window_title) LIKE '%サンデーうぇぶり%'
              OR LOWER(raw_app_name) LIKE '%マンガワン%' OR LOWER(raw_window_title) LIKE '%マンガワン%'
              OR LOWER(raw_app_name) LIKE '%ヤンジャン%' OR LOWER(raw_window_title) LIKE '%ヤンジャン%'
              -- 2026-08-29 追加。未分類のまま 'ネットサーフィン' に落ちていた漫画アプリ。
              -- ジャンプ＋ 186.8分 / マガポケ 98.4分 / ゼブラック 34.8分（10日間）。
              -- 漫画のブロック効果を測るときにこれらが見えていなかった。
              OR LOWER(raw_app_name) LIKE '%ジャンプ＋%' OR LOWER(raw_window_title) LIKE '%ジャンプ＋%'
              OR LOWER(raw_app_name) LIKE '%マガポケ%' OR LOWER(raw_window_title) LIKE '%マガポケ%'
              OR LOWER(raw_app_name) LIKE '%ゼブラック%' OR LOWER(raw_window_title) LIKE '%ゼブラック%'
              OR LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' THEN 'MANGA'
            WHEN LOWER(raw_app_name) LIKE '%chrome%' OR LOWER(raw_app_name) LIKE '%edge%' OR LOWER(raw_app_name) LIKE '%brave%' THEN 'BROWSING'
            WHEN usage_type = 'gaming' THEN 'GAME'
            WHEN LOWER(raw_window_title) LIKE '%amazon%' OR LOWER(raw_window_title) LIKE '%楽天市場%' THEN 'LIFE'
            WHEN LOWER(raw_app_name) LIKE '%uber eats%' THEN 'LIFE'
            -- 記録・健康管理アプリは娯楽ではない
            WHEN LOWER(raw_app_name) LIKE '%あすけん%' OR LOWER(raw_app_name) LIKE '%pokémon sleep%'
              OR LOWER(raw_app_name) LIKE '%pokemon sleep%' THEN 'LIFE'
            ELSE 'BROWSING'
        END AS cat_main,

        CASE
            WHEN usage_type = 'work' THEN '業務'
            WHEN LOWER(raw_app_name) LIKE '%ghostty%' OR LOWER(raw_app_name) LIKE '%vscode%' THEN '個人開発(コーディング)'
            WHEN LOWER(raw_app_name) LIKE '%notion%' OR LOWER(raw_window_title) LIKE '%notion%' THEN 'notion'
            WHEN LOWER(raw_app_name) LIKE '%gogh%' OR LOWER(raw_window_title) LIKE '%gogh%' THEN 'Gogh'
            WHEN LOWER(raw_app_name) LIKE '%chatgpt%' OR LOWER(raw_window_title) LIKE '%chatgpt%'
              OR LOWER(raw_app_name) LIKE '%gemini%' OR LOWER(raw_window_title) LIKE '%gemini%' THEN '個人開発(AIペアプロ)'
            WHEN LOWER(raw_window_title) LIKE '%life dashboard%'
              OR LOWER(raw_app_name) LIKE '%life dashboard%' THEN '生活管理'
            WHEN LOWER(raw_app_name) LIKE '%.mynet%' OR LOWER(raw_window_title) LIKE '%.mynet%'
              OR LOWER(raw_app_name) LIKE '%rancher%' OR LOWER(raw_window_title) LIKE '%rancher%'
              OR LOWER(raw_app_name) LIKE '%superset%' OR LOWER(raw_window_title) LIKE '%superset%'
              OR LOWER(raw_app_name) LIKE '%activitywatch%' OR LOWER(raw_window_title) LIKE '%activitywatch%'
              OR LOWER(raw_app_name) LIKE '%prefect%' OR LOWER(raw_window_title) LIKE '%prefect%'
              OR LOWER(raw_app_name) LIKE '%minio%' OR LOWER(raw_window_title) LIKE '%minio%'
              OR LOWER(raw_app_name) LIKE '%localhost%' OR LOWER(raw_window_title) LIKE '%localhost%' THEN '個人開発(自宅インフラ)'
            WHEN LOWER(raw_app_name) LIKE '%qiita%' OR LOWER(raw_window_title) LIKE '%qiita%' THEN 'qiita'
            WHEN LOWER(raw_app_name) LIKE '%udemy%' OR LOWER(raw_window_title) LIKE '%udemy%' THEN '学習'
            WHEN LOWER(raw_window_title) LIKE '%siken.com%'
              OR raw_window_title LIKE '%過去問道場%'
              OR raw_window_title LIKE '%技術者試験.com%' THEN '学習'
            WHEN LOWER(raw_app_name) LIKE '%kindle%' OR LOWER(raw_window_title) LIKE '%kindle%' THEN 'Kindle'
            WHEN LOWER(raw_app_name) LIKE '%slack%' OR LOWER(raw_app_name) LIKE '%discord%' THEN 'コミュニティ'
            WHEN LOWER(raw_app_name) LIKE '%x.com%' OR LOWER(raw_window_title) LIKE '%x.com%'
              OR LOWER(raw_window_title) LIKE '%twitter.com%' OR LOWER(raw_window_title) LIKE '% / x %' THEN 'SNS'
            WHEN LOWER(raw_app_name) LIKE '%youtube music%' OR LOWER(raw_window_title) LIKE '%youtube music%' THEN 'YouTube Music'
            WHEN LOWER(raw_app_name) LIKE '%amazon music%' OR LOWER(raw_window_title) LIKE '%amazon music%' THEN 'Amazon Music'
            WHEN LOWER(raw_app_name) LIKE '%spotify%' OR LOWER(raw_window_title) LIKE '%spotify%' THEN 'Spotify'
            WHEN LOWER(raw_app_name) LIKE '%apple music%' THEN 'Apple Music'
            WHEN LOWER(raw_app_name) LIKE '%u-next%' OR LOWER(raw_window_title) LIKE '%u-next%' THEN 'U-NEXT'
            WHEN LOWER(raw_app_name) LIKE '%dazn%' OR LOWER(raw_window_title) LIKE '%dazn%' THEN 'スポーツ観戦'
            WHEN LOWER(raw_app_name) LIKE '%youtube%' OR LOWER(raw_window_title) LIKE '%youtube%' THEN 'YouTube'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ動画%' OR LOWER(raw_app_name) LIKE '%ニコニコ生放送%' THEN 'niconico'
            WHEN LOWER(raw_app_name) LIKE '%twitch%' OR LOWER(raw_window_title) LIKE '%twitch%' THEN 'Twitch'
            WHEN LOWER(raw_app_name) LIKE '%ニコニコ漫画%' OR LOWER(raw_window_title) LIKE '%ニコニコ漫画%' THEN 'ニコニコ漫画'
            WHEN LOWER(raw_app_name) LIKE '%コミックdays%' OR LOWER(raw_window_title) LIKE '%コミックdays%' THEN 'コミックDAYS'
            WHEN LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' OR LOWER(raw_window_title) LIKE '%サンデーうぇぶり%' THEN 'サンデーうぇぶり'
            WHEN LOWER(raw_app_name) LIKE '%マンガワン%' OR LOWER(raw_window_title) LIKE '%マンガワン%' THEN 'マンガワン'
            WHEN LOWER(raw_app_name) LIKE '%ヤンジャン%' OR LOWER(raw_window_title) LIKE '%ヤンジャン%' THEN 'ヤンジャン＋'
            WHEN LOWER(raw_app_name) LIKE '%ジャンプ＋%' OR LOWER(raw_window_title) LIKE '%ジャンプ＋%' THEN 'ジャンプ＋'
            WHEN LOWER(raw_app_name) LIKE '%マガポケ%' OR LOWER(raw_window_title) LIKE '%マガポケ%' THEN 'マガポケ'
            WHEN LOWER(raw_app_name) LIKE '%ゼブラック%' OR LOWER(raw_window_title) LIKE '%ゼブラック%' THEN 'ゼブラック'
            WHEN LOWER(raw_app_name) LIKE '%サンデーうぇぶり%' THEN 'サンデーうぇぶり'
            WHEN LOWER(raw_app_name) LIKE '%prime video%' OR LOWER(raw_window_title) LIKE '%prime video%' THEN 'Prime Video'
            WHEN LOWER(raw_app_name) LIKE '%abema%' OR LOWER(raw_window_title) LIKE '%abema%' THEN 'ABEMA'
            WHEN LOWER(raw_app_name) LIKE '%fotmob%' THEN 'スポーツ観戦'
            WHEN usage_type = 'gaming' THEN 'ゲーム'
            WHEN LOWER(raw_window_title) LIKE '%amazon%' OR LOWER(raw_window_title) LIKE '%楽天市場%' THEN 'ネットショッピング'
            WHEN LOWER(raw_app_name) LIKE '%uber eats%' THEN 'Uber Eats'
            WHEN LOWER(raw_app_name) LIKE '%あすけん%' THEN '食事記録'
            WHEN LOWER(raw_app_name) LIKE '%pokémon sleep%' OR LOWER(raw_app_name) LIKE '%pokemon sleep%' THEN '睡眠記録'
            ELSE 'ネットサーフィン'
        END AS cat_sub
    FROM split_events
),

-- split_eventsのLEFT JOINで1つのwindow_eventが複数afk_eventと結合し
-- 同一event_pkが重複する場合にMERGE_TARGET_ROW_MULTIPLE_MATCHESを防ぐ
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY event_pk ORDER BY start_ts) AS rn
    FROM categorized
),

-- ─────────────────────────────────────────────────────────────
-- DEVELOP を「個人開発(DEVELOP)」と「勉強(STUDY)」に分ける
--
-- ★なぜ必要か★
-- 資格勉強と個人開発がどちらも DEVELOP で、開発への逃避が勉強の頑張りとして
-- 表示されていた。本人の言葉:「資格の勉強が全然進んでいないことを相談したいのに、
-- 開発に逃避しているがカテゴリが同じせいですごく頑張っている判定されちゃってる」
--
-- ★アプリ名だけでは原理的に分けられない★
-- 実測（30日・DEVELOP 48.7h）: Ghostty 15.6h は確実に開発、
-- Udemy と 技術者試験.com は確実に勉強。しかし Gemini 15.7h と Notion 5.8h が
-- 両用途で、これが最大の塊。勉強のやり方が「本や過去問を解きながら Notion に
-- ノートを取り、気になった部分を Gemini に解説させる」なので、
-- Gemini 主体の時間は勉強でもある。
-- さらに **signal の意味が時期で変わっている**（以前は Gemini をほぼ開発に使っていた）。
-- 静的なアプリ名ルールでは解けない。
--
-- ★採った方法: ブロック単位の昇格★
-- 勉強と開発はまとまった時間で行われるので、連続ブロックを単位にする。
-- ベースは「勉強」。ブロック内のコーディング系が DEVELOP 時間の 20% 以上なら、
-- そのブロックの曖昧な時間を全部「開発」に上げる。
--
-- しきい値の実測:
--   10% → 開発 28.0h / 勉強 20.7h   ターミナルを少し触った勉強が開発に流れる
--   20% → 開発 25.0h / 勉強 23.7h   ← 採用
--   50% → 開発 14.4h / 勉強 34.4h   Ghostty 中心の開発が勉強に残る
--
-- ★判別不能カテゴリは作らない★（本人の要望: 正確に測れないとモニタリングにならない）
-- 代わりに確定しているものは常に確定させ、曖昧なものだけブロックに従わせる。
--
-- ★空白 15分 で区切る理由★
-- AW の欠測より長く、作業の中断として妥当。長くすると1ブロックが数時間になり
-- 1ラベルで塗るのが無理になる（実測で 5.7h のブロックが出て、その中に
-- コーディングも過去問も混在していた）。
-- ─────────────────────────────────────────────────────────────
-- ★窓関数を2段に分ける★
-- SUM(... LAG() OVER ...) OVER () は Trino で
-- 「Cannot nest window functions」エラーになる（dbt compile では通るが実行で落ちる）。
--
-- ★端末ごとに区切る（PARTITION BY raw_hostname）★
-- 端末をまたいで1本の時系列にすると、Mac・スマホ・Windows のイベントが
-- 交互に並んで空白が埋まり、ブロックが繋がり続ける。
-- 実測: 端末を混ぜると 74ブロックが平均199分・最大825分（13.7時間）になり、
-- 1ラベルで塗る意味が失われていた。端末ごとなら平均26分に収まる。
dev_gap AS (
    SELECT *,
        CASE
            WHEN LAG(end_ts) OVER (PARTITION BY raw_hostname ORDER BY start_ts) IS NULL THEN 1
            WHEN date_diff('minute',
                     LAG(end_ts) OVER (PARTITION BY raw_hostname ORDER BY start_ts),
                     start_ts) > 15 THEN 1
            ELSE 0
        END AS is_new_block
    FROM deduplicated
    WHERE rn = 1 AND is_afk = false
),
dev_block AS (
    SELECT *,
        raw_hostname || '#' || CAST(
            SUM(is_new_block) OVER (PARTITION BY raw_hostname ORDER BY start_ts) AS VARCHAR
        ) AS block_id
    FROM dev_gap
),
dev_block_ratio AS (
    SELECT block_id,
        SUM(IF(cat_main = 'DEVELOP', date_diff('second', start_ts, end_ts), 0)) AS develop_sec,
        SUM(IF(LOWER(raw_app_name) LIKE '%ghostty%'
                OR LOWER(raw_app_name) LIKE '%vscode%'
                OR LOWER(raw_app_name) LIKE '%cursor%'
                OR LOWER(raw_app_name) LIKE '%iterm%'
                OR LOWER(raw_app_name) LIKE '%terminal%',
               date_diff('second', start_ts, end_ts), 0)) AS coding_sec
    FROM dev_block
    GROUP BY block_id
),
-- ★dev_kind 列ではなく cat_main を分ける理由★
-- 本人の判断:「学習と個人開発系は別物として分けましょう」「タイムライン表示は分けたい」。
-- 別物として扱うなら別カテゴリにするのが素直で、色・優先度・スコアがそのまま付く。
-- 派生列(dev_kind)にすると、cat_main='DEVELOP' で絞る既存コードが勉強を巻き込み続け、
-- 「同じ事実の出どころが2つ」になる。
kinded AS (
    SELECT d.event_pk, d.event_date_jst, d.source_system, d.source_detail,
        d.start_ts, d.end_ts, d.is_afk, d.raw_app_name, d.raw_window_title,
        d.raw_usage_type, d.raw_hostname,
        -- ★cat_sub も実態に合わせる★
        -- cat_main だけ STUDY にすると cat_sub が「個人開発(AIペアプロ)」のまま残り、
        -- 「STUDY / 個人開発(AIペアプロ)」という矛盾した組み合わせが表示される。
        -- Gemini/ChatGPT/Notion は用途で名前が変わるので、判定結果に合わせて付け替える。
        CASE
            WHEN d.cat_main <> 'DEVELOP' THEN d.cat_sub
            WHEN d.cat_sub IN ('個人開発(コーディング)', '個人開発(自宅インフラ)', '学習', 'qiita')
                THEN d.cat_sub
            -- 曖昧なもの（AIペアプロ / notion）はブロック判定に従って名前も変える
            WHEN r.develop_sec > 0
                 AND CAST(r.coding_sec AS DOUBLE) / r.develop_sec >= 0.20
                THEN CASE WHEN d.cat_sub = 'notion' THEN '個人開発(notion)'
                          ELSE '個人開発(AIペアプロ)' END
            ELSE CASE WHEN d.cat_sub = 'notion' THEN '学習(ノート)'
                      ELSE '学習(AI質問)' END
        END AS cat_sub,
        CASE
            WHEN d.cat_main <> 'DEVELOP' THEN d.cat_main
            -- 確定しているものはブロックに関係なく確定させる
            WHEN d.cat_sub IN ('個人開発(コーディング)', '個人開発(自宅インフラ)') THEN 'DEVELOP'
            WHEN d.cat_sub IN ('学習', 'qiita') THEN 'STUDY'
            -- 曖昧なもの（AIペアプロ = Gemini/ChatGPT、notion）だけブロックに従う
            WHEN r.develop_sec > 0
                 AND CAST(r.coding_sec AS DOUBLE) / r.develop_sec >= 0.20 THEN 'DEVELOP'
            ELSE 'STUDY'
        END AS cat_main
    FROM deduplicated d
    -- AFK を含む全行に dev_kind を付ける必要があるので、
    -- ブロック判定（AFK 除外）とは LEFT JOIN で結ぶ
    LEFT JOIN dev_block b
           ON d.event_pk = b.event_pk
    LEFT JOIN dev_block_ratio r
           ON b.block_id = r.block_id
    WHERE d.rn = 1
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
        -- 勉強は開発と同じ優先度。どちらも「意図して集中している時間」なので
        -- 15分スロットで娯楽に負けてはいけない。
        WHEN cat_main = 'STUDY' THEN 50
        WHEN cat_main = 'SOCIAL' THEN 40
        WHEN raw_usage_type = 'gaming' AND cat_sub = 'ゲーム' THEN 55
        WHEN cat_main = 'MUSIC' THEN 20
        WHEN cat_main = 'ENTERTAINMENT' THEN 35
        WHEN cat_main = 'LIFE' THEN 30
        ELSE 25
    END AS priority
FROM kinded
