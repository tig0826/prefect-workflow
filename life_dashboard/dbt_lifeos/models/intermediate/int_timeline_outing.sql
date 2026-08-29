-- ★merge ではなく delete+insert★
-- 下の transit は隣接セグメントを連結して1イベントにするため、上流に1件増えると
-- グループの境界が動き event_pk が変わる。merge だと古い行が残って二重計上になる。
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date_jst',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['event_date_jst']
) }}

-- 「家にいなかった時間」を行動イベントとして出すモデル。cat_main を2つに分ける。
--
--   OUTING  / 外出先滞在  … 自宅以外の場所に留まっていた
--   TRANSIT / 移動中      … 目的地の間を実際に移動していた
--
-- ★この2つを混ぜてはいけない★
-- 以前は両方 'OUTING'（UI 表記「外出/移動」）だったが、それだと実家に帰省した
-- 数日間が丸ごと「移動」に見えてしまう。実際に動いていた時間だけが移動である。
-- ─── 以前のバグ ───────────────────────────────────────────
-- 旧実装は activity を activity_type IN ('IN_VEHICLE','ON_BICYCLE') で拾っていたが、
-- 実データに入っている値は WALKING / IN_TRAIN / IN_BUS / IN_PASSENGER_VEHICLE /
-- IN_SUBWAY / IN_TRAM / IN_FERRY で、この2つは**1件も存在しなかった**。
-- 結果 156件すべてが「外出先滞在」になり、移動時間が丸ごと欠落していた。
--
-- ─── 移動の判定条件 ─────────────────────────────────────────
-- Google Maps のタイムラインが出している移動をそのまま使う。距離や速度で
-- 絞ってはいけない。一度 distance_meters > 300 で絞ったが、それは
-- **バス停までの徒歩を消していた**:
--
--   08-20 17:07-17:12 WALKING 234m   ← バス停まで（消えていた）
--   08-20 17:12-17:39 IN_BUS  4230m
--   08-20 17:39-17:47 WALKING 264m   ← バス停から（消えていた）
--
-- 実際に壊れているのは 240件中4件だけで、いずれも所要が異常に長い
-- （8,546分/105m、3,846分/9m、868分、433分）。次に長いのは正当な
-- IN_TRAIN 299分なので、360分で切れば誤って落とすものが無い。
--
-- 検証: この条件だと Fitbit 歩数との相関は r=0.836（2026-08, n=28）。
-- 距離・速度で絞った版は 0.814 だったので、絞らない方が実態に近い。
--
-- 自宅座標での判定は採らない（引っ越し・外泊で壊れる）。そもそも
-- Google 側が屋内滞在を visit として分けてくれているので不要。
--
-- ─── 15分未満のギャップを繋ぐ理由 ───────────────────────────
-- 徒歩→バス→徒歩が3セグメントに割れるため、繋がないと1回の移動が
-- 帯グラフ上で分断される。実測の同日内ギャップは中央値12分。
-- 連結後は126区間・中央23分になり、上の例も
-- 「17:07-17:47 40分 WALKING+IN_BUS+WALKING」と1回の移動にまとまる。
--
-- ─── priority の設計 ──────────────────────────────────────
-- 旧実装は両方 20（MUSIC と並んで最下位）だったため、移動中にスマホを
-- 触った瞬間に MEDIA/BROWSING に負けて帯が途切れていた。
-- 移動と滞在で扱いを分ける:
--
--   移動中     = 85（睡眠100・運動90 にのみ譲る）
--       移動しながら音楽を聴いても「音楽」ではなく「移動」と出したい。
--       スマホを触っても移動していることは変わらない。
--
--   外出先滞在 = 20（最下位のまま。あらゆる活動に譲る）
--       帰省先や外出先で何をしていたかが見えないと後から思い出せない。
--       滞在中は「何をしていたか」が見えないと後から思い出せない。
--       活動ログが無いスロットでは OUTING が唯一のイベントなので
--       結局勝ち、「データなし」にはならない。
--
-- なお mrt_behavior_slots_15m が帯とは別レイヤーで出す連続ラベルは
-- 移動(TRANSIT)だけを対象にしている。滞在まで含めると上記の
-- 「帰省が全部移動」問題が下線側で再発するため。

{% set reprocess_days = var('reprocess_days', 14) %}
{% set transit_max_minutes = 360 %}
{% set transit_bridge_gap_min = 15 %}

WITH src AS (
    SELECT
        segment_pk, event_date_jst, start_ts_jst, end_ts_jst,
        segment_type, place_semantic_type, activity_type, distance_meters
    FROM {{ ref('timeline_segments') }}
    {% if is_incremental() %}
    WHERE event_date_jst >= CAST(date_add('day', -{{ reprocess_days }}, CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS DATE)) AS DATE)
    {% endif %}
),

-- ① 外出先滞在: 自宅ラベル（TYPE_HOME / INFERRED_HOME 等）以外の visit
visits AS (
    SELECT
        segment_pk AS event_pk,
        event_date_jst,
        'timeline_visit' AS source_detail,
        CAST(start_ts_jst AS TIMESTAMP) AS start_ts,
        CAST(end_ts_jst AS TIMESTAMP) AS end_ts,
        '外出先滞在' AS cat_sub,
        20 AS priority,
        'OUTING' AS cat_main
    FROM src
    WHERE segment_type = 'visit'
      AND (place_semantic_type IS NULL OR place_semantic_type NOT LIKE '%HOME%')
),

-- ② 移動: ノイズを落としてから連結する
transit_clean AS (
    SELECT
        start_ts_jst, end_ts_jst, activity_type, distance_meters,
        date_diff('second', start_ts_jst, end_ts_jst) AS dur_sec
    FROM src
    WHERE segment_type = 'activity'
      -- 壊れた4件（8,546分など）だけを落とす。距離・速度では絞らない。
      AND date_diff('second', start_ts_jst, end_ts_jst) BETWEEN 1 AND {{ transit_max_minutes * 60 }}
),

-- 直前までの最大 end を見てギャップを測る（セグメントが重なっても崩れないように）
transit_gapped AS (
    SELECT *,
        MAX(end_ts_jst) OVER (
            ORDER BY start_ts_jst
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_end_ts
    FROM transit_clean
),

transit_flagged AS (
    SELECT *,
        CASE
            WHEN prev_end_ts IS NULL THEN 1
            WHEN date_diff('second', prev_end_ts, start_ts_jst) < {{ transit_bridge_gap_min * 60 }} THEN 0
            ELSE 1
        END AS is_group_start
    FROM transit_gapped
),

transit_grouped AS (
    SELECT *,
        SUM(is_group_start) OVER (ORDER BY start_ts_jst ROWS UNBOUNDED PRECEDING) AS grp
    FROM transit_flagged
),

-- グループ内で最も長く乗っていた手段を代表にする
transit_mode AS (
    SELECT grp, activity_type,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY SUM(dur_sec) DESC, activity_type) AS rn
    FROM transit_grouped
    GROUP BY grp, activity_type
),

transit AS (
    SELECT
        to_hex(md5(to_utf8('transit|' || CAST(MIN(g.start_ts_jst) AS VARCHAR)))) AS event_pk,
        CAST(MIN(g.start_ts_jst) AS DATE) AS event_date_jst,
        'timeline_activity' AS source_detail,
        CAST(MIN(g.start_ts_jst) AS TIMESTAMP) AS start_ts,
        CAST(MAX(g.end_ts_jst) AS TIMESTAMP) AS end_ts,
        CASE MAX(m.activity_type)
            WHEN 'IN_TRAIN' THEN '移動中(電車)'
            WHEN 'IN_SUBWAY' THEN '移動中(電車)'
            WHEN 'IN_TRAM' THEN '移動中(電車)'
            WHEN 'IN_BUS' THEN '移動中(バス)'
            WHEN 'IN_PASSENGER_VEHICLE' THEN '移動中(車)'
            WHEN 'IN_FERRY' THEN '移動中(船)'
            WHEN 'WALKING' THEN '移動中(徒歩)'
            ELSE '移動中'
        END AS cat_sub,
        85 AS priority,
        'TRANSIT' AS cat_main
    FROM transit_grouped g
    JOIN transit_mode m ON m.grp = g.grp AND m.rn = 1
    GROUP BY g.grp
)

SELECT
    event_pk,
    event_date_jst,
    'timeline' AS source_system,
    source_detail,
    start_ts,
    end_ts,
    CAST(NULL AS VARCHAR) AS raw_app_name,
    CAST(NULL AS VARCHAR) AS raw_window_title,
    CAST(NULL AS VARCHAR) AS raw_usage_type,
    cat_main,
    cat_sub,
    priority
FROM (
    SELECT event_pk, event_date_jst, source_detail, start_ts, end_ts, cat_sub, priority, cat_main FROM visits
    UNION ALL
    SELECT event_pk, event_date_jst, source_detail, start_ts, end_ts, cat_sub, priority, cat_main FROM transit
)
