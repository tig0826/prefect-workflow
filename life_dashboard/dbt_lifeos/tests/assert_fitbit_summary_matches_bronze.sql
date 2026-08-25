-- bronze に値があるのに silver が NULL のままになっている日を検出する。
--
-- 個々の API 取得失敗は 15 分後の実行で bronze が復旧し、silver も
-- reprocess_days の読み直しで自己修復するので、それ自体は通知しない
-- （直近24時間でも十数回起きるためノイズになる）。ここで検出したいのは
-- 「bronze は正常なのに silver に取り込まれない」= 恒久的な取り込み失敗で、
-- 2026-07 のトークン切れ後に bronze だけ backfill されて silver が
-- NULL のまま8日間放置されていた状態がまさにこれ。
--
-- 当日と前日は取得途中なので除外し、確定した 2〜7 日前だけを対象にする。
WITH bronze AS (
    SELECT
        dt,
        json_extract_scalar(
            json_extract_scalar(raw_json, '$.raw_json'), '$.activities.summary.steps'
        ) AS steps,
        json_extract_scalar(
            json_extract_scalar(raw_json, '$.raw_json'), '$.sleep.summary.totalMinutesAsleep'
        ) AS minutes_asleep
    FROM {{ source('hive_life_bronze', 'fitbit_external') }}
    WHERE dt >= CAST(date_add('day', -7, current_date) AS VARCHAR)
      AND dt <= CAST(date_add('day', -2, current_date) AS VARCHAR)
)

SELECT
    bronze.dt,
    bronze.steps AS bronze_steps,
    silver.steps AS silver_steps,
    bronze.minutes_asleep AS bronze_minutes_asleep,
    silver.total_minutes_asleep AS silver_minutes_asleep
FROM bronze
LEFT JOIN {{ ref('fitbit_summary') }} AS silver
    ON silver.dt = bronze.dt
WHERE (bronze.steps IS NOT NULL AND silver.steps IS NULL)
   OR (bronze.minutes_asleep IS NOT NULL AND silver.total_minutes_asleep IS NULL)
