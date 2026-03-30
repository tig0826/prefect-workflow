{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='meal_pk',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        dt,
        meal_records
    FROM {{ source('hive_life_bronze', 'asken_external') }}
    {% if is_incremental() %}
    WHERE dt >= (SELECT MAX(dt) FROM {{ this }})
    {% endif %}
),

all_meals AS (
    SELECT dt, '朝食' AS meal_type, meal_records[1][2] AS items FROM base
    UNION ALL
    SELECT dt, '昼食' AS meal_type, meal_records[2][2] AS items FROM base
    UNION ALL
    SELECT dt, '夕食' AS meal_type, meal_records[3][2] AS items FROM base
    UNION ALL
    SELECT dt, '間食' AS meal_type, meal_records[4][2] AS items FROM base
    -- 運動（meal_records[5]）はFitbit側で管理するため除外
),

unnested_items AS (
    SELECT
        dt,
        meal_type,
        i.menu_name,
        i.amount,
        CAST(i.calories AS DOUBLE) AS calories_kcal,
        row_number() OVER(PARTITION BY dt, meal_type) as item_seq
    FROM all_meals
    CROSS JOIN UNNEST(items) AS i(menu_name, amount, calories)
    WHERE cardinality(items) > 0
)

SELECT
    to_hex(md5(to_utf8(dt || meal_type || menu_name || CAST(item_seq AS VARCHAR)))) AS meal_pk,
    dt,
    meal_type,
    menu_name,
    amount,
    calories_kcal
FROM unnested_items
WHERE menu_name IS NOT NULL
