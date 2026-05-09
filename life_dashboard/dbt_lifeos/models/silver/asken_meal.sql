{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='meal_pk',
    table_type='iceberg',
    format='parquet'
) }}

WITH raw_asken AS (
    SELECT
        CAST(dt AS DATE) AS meal_date,
        meal_records
    FROM {{ source('hive_life_bronze', 'asken_external') }}
    {% if is_incremental() %}
    WHERE CAST(dt AS DATE) >= CAST((SELECT MAX(meal_date) FROM {{ this }}) AS DATE)
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
