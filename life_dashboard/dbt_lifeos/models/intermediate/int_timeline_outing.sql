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
-- ─── 移動の判定条件（実データから決めた）─────────────────────
-- WALKING をそのまま信じると壊れる。GPS が自宅で微動するのを Google が
-- 「徒歩」と解釈するため、実測で最大 8,547分 / 移動距離 105m という
-- セグメントが存在した（240件中75件・計14,974分がこの種のノイズ）。
-- かといって WALKING を全部捨てると本物の徒歩外出が消える。
--
-- 自宅座標での判定は採らなかった。引っ越し・外泊で壊れるため。
-- 代わりにセグメント自身の性質だけで切る:
--
--   distance_meters > 300   … 屋内の微動は総移動距離が伸びない
--   1.0 <= 速度 <= 300 km/h … 下限は「停留を含んで連結されたルート」を除去
--                             （除外された75件の速度中央値は 1.40km/h）
--   所要時間 <= 360分       … 異常に長い連結セグメントの保険
--
-- 検証: この条件で残した時間は Fitbit の歩数と r=0.814（8月, n=28）で一致する。
-- 採用後の速度中央値は 7.17km/h・時間中央値 16分 と移動として妥当な値になる。
--
-- ─── 15分未満のギャップを繋ぐ理由 ───────────────────────────
-- GPS のサンプリングが粗く、1回の外出が複数セグメントに割れる。実測の
-- 同日内ギャップは中央値12分で、116件中61件が15分未満だった。
-- 帯グラフ上で1スロットだけ穴が空くのを避けるため、15分未満は同一移動として繋ぐ。
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
{% set transit_min_distance_m = 300 %}
{% set transit_min_kmh = 1.0 %}
{% set transit_max_kmh = 300 %}
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
      AND distance_meters > {{ transit_min_distance_m }}
      AND date_diff('second', start_ts_jst, end_ts_jst) BETWEEN 1 AND {{ transit_max_minutes * 60 }}
      AND distance_meters / date_diff('second', start_ts_jst, end_ts_jst) * 3.6
            BETWEEN {{ transit_min_kmh }} AND {{ transit_max_kmh }}
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
