-- ★merge ではなく delete+insert を使う理由★
--
-- あすけんの日次データは「その日のスナップショット」であり、行が減ることがある
-- （ユーザーが入力を削除する）。merge + unique_key='meal_pk' は
-- 「あるものを追加・更新」しかしないため、**削除された行が永久に残る**。
--
-- 実害（2026-08-27）: 夕食のケーキ2品を削除したのに silver に残り、
-- 品目の合計 2549kcal / 栄養テーブル 1018kcal という矛盾が発生した
-- （栄養は日次1行なので merge でも正しく上書きされていた）。
-- その結果チャットが存在しない食事を根拠にアドバイスしていた。
--
-- delete+insert + unique_key='meal_date' で、対象日の行をまとめて入れ替える。
--
-- 注意: config() は Jinja 式なので中に -- コメントを書くと
-- 「invalid syntax for function call expression」で落ちる。説明はここに書く。
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='meal_date',
    table_type='iceberg',
    format='parquet'
) }}

{% set reprocess_days = var('reprocess_days', 14) %}

WITH raw_asken AS (
    SELECT
        CAST(dt AS DATE) AS meal_date,
        meal_records
    FROM {{ source('hive_life_bronze', 'asken_external') }}
    {% if is_incremental() %}
    -- MAX(meal_date) 起点だと、過去日の編集（削除・追加）を取り込めない。
    -- あすけんは後から入力を直すことがあるので直近 N 日を読み直す。
    WHERE CAST(dt AS DATE) >= date_add('day', -{{ reprocess_days }}, current_date)
    {% endif %}
),

all_meals AS (
    SELECT meal_date, '朝食' AS meal_type, meal_records[1][2] AS items FROM raw_asken
    UNION ALL
    SELECT meal_date, '昼食' AS meal_type, meal_records[2][2] AS items FROM raw_asken
    UNION ALL
    SELECT meal_date, '夕食' AS meal_type, meal_records[3][2] AS items FROM raw_asken
    UNION ALL
    SELECT meal_date, '間食' AS meal_type, meal_records[4][2] AS items FROM raw_asken
    -- meal_records[5] は運動ログ（Fitbit側で管理するため除外）
),

unnested_items AS (
    SELECT
        meal_date,
        meal_type,
        i.menu_name,
        i.amount,
        CAST(i.calories AS DOUBLE) AS calories_kcal,
        row_number() OVER(PARTITION BY meal_date, meal_type) as item_seq
    FROM all_meals
    CROSS JOIN UNNEST(items) AS i(menu_name, amount, calories)
    WHERE cardinality(items) > 0
)

SELECT
    to_hex(md5(to_utf8(CAST(meal_date AS VARCHAR) || meal_type || menu_name || CAST(item_seq AS VARCHAR)))) AS meal_pk,
    meal_date,
    meal_type,
    menu_name,
    amount,
    calories_kcal
FROM unnested_items
WHERE menu_name IS NOT NULL
