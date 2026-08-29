{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='target_date',
    table_type='iceberg',
    format='parquet'
) }}

-- 回復・自律神経系の指標（HRV / 呼吸数 / SpO2 / 皮膚温 / VO2max / AZM）。
-- 2026-08-27 の Fitbit 再認証でスコープを追加して取れるようになった分。
--
-- ★欠測の扱いが最重要★
--   HRV・呼吸数・SpO2・皮膚温はいずれも**睡眠中の計測**なので、
--   時計を外した夜・寝ていない夜にまとめて落ちる。実測カバレッジは60日で
--   HRV 55% / 呼吸数 55% / SpO2 57% / 皮膚温 55% で、**欠測日が完全に一致する**
--   （8/14, 8/16-17, 8/20-21, 8/23 など。8/21 は睡眠0h の日）。
--   つまり「調子が悪い日に限って欠ける」MNAR であり、単独では主指標にできない。
--   **NULL を 0 で埋めたり「改善」と解釈してはいけない。**
--
--   4指標が揃った日は複合として見る価値がある（相関した4指標が同方向に動くのは
--   ノイズでは起きにくい）。そのため signals_available を持たせて、
--   下流で「3つ以上揃った日だけ評価する」判定ができるようにする。
--
--   VO2max はカバレッジ100%だが「35-39」のようなレンジ文字列で、
--   数週間単位でしか動かないので日次の状態把握には使えない。

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_fitbit AS (
    SELECT
        dt,
        json_extract_scalar(raw_json, '$.raw_json') AS real_json
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    {% if is_incremental() %}
    WHERE dt >= (
        SELECT CAST(date_add('day', -{{ reprocess_days }}, CAST(MAX(target_date) AS DATE)) AS VARCHAR)
        FROM {{ this }}
    )
    {% endif %}
),

extracted AS (
    SELECT
        CAST(dt AS DATE) AS target_date,
        CAST(json_extract_scalar(real_json, '$.hrv.hrv[0].value.dailyRmssd') AS DOUBLE) AS hrv_daily_rmssd,
        CAST(json_extract_scalar(real_json, '$.hrv.hrv[0].value.deepRmssd') AS DOUBLE) AS hrv_deep_rmssd,
        CAST(json_extract_scalar(real_json, '$.br.br[0].value.breathingRate') AS DOUBLE) AS breathing_rate,
        CAST(json_extract_scalar(real_json, '$.spo2.value.avg') AS DOUBLE) AS spo2_avg,
        CAST(json_extract_scalar(real_json, '$.spo2.value.min') AS DOUBLE) AS spo2_min,
        CAST(json_extract_scalar(real_json, '$.spo2.value.max') AS DOUBLE) AS spo2_max,
        -- nightlyRelative は「その人の基準からの相対値(℃)」。絶対温度ではない。
        CAST(json_extract_scalar(real_json, '$.skin_temp.tempSkin[0].value.nightlyRelative') AS DOUBLE) AS skin_temp_relative,
        json_extract_scalar(real_json, '$.cardio.cardioScore[0].value.vo2Max') AS vo2_max_range,
        -- Trino の JSONPath はハイフン入りキーに二重引用符記法を使えない（Invalid JSON path）。
        -- 括弧記法で書く。
        CAST(json_extract_scalar(real_json, '$.azm["activities-active-zone-minutes"][0].value.activeZoneMinutes') AS INTEGER) AS azm_total,
        CAST(json_extract_scalar(real_json, '$.azm["activities-active-zone-minutes"][0].value.fatBurnActiveZoneMinutes') AS INTEGER) AS azm_fat_burn,
        CAST(json_extract_scalar(real_json, '$.azm["activities-active-zone-minutes"][0].value.cardioActiveZoneMinutes') AS INTEGER) AS azm_cardio,
        CAST(json_extract_scalar(real_json, '$.azm["activities-active-zone-minutes"][0].value.peakActiveZoneMinutes') AS INTEGER) AS azm_peak
    FROM raw_fitbit
),

deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY target_date ORDER BY target_date) AS rn
    FROM extracted
    WHERE target_date IS NOT NULL
)

SELECT
    target_date,
    hrv_daily_rmssd,
    hrv_deep_rmssd,
    breathing_rate,
    spo2_avg,
    spo2_min,
    spo2_max,
    skin_temp_relative,
    vo2_max_range,
    azm_total,
    azm_fat_burn,
    azm_cardio,
    azm_peak,
    -- 睡眠中計測の4指標のうち何個取れているか。
    -- 下流はこれで「複合として評価してよい日」を判定する（3以上を推奨）。
    (CASE WHEN hrv_daily_rmssd   IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN breathing_rate    IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN spo2_avg          IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN skin_temp_relative IS NOT NULL THEN 1 ELSE 0 END) AS signals_available,
    CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS TIMESTAMP) AS transformed_at_jst
FROM deduped
WHERE rn = 1
