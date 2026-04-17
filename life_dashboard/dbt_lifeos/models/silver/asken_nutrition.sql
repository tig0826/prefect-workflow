{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dt',
    table_type='iceberg'
) }}

WITH base AS (
    SELECT
        CAST(dt AS DATE) AS dt,
        nutrition_summary
    FROM {{ source('hive_life_bronze', 'asken_external') }}
    {% if is_incremental() %}
    WHERE CAST(dt AS DATE) >= current_date - INTERVAL '3' DAY
    {% endif %}
),

extracted AS (
    SELECT
        dt,
        CAST(element_at(nutrition_summary, 'エネルギー').value AS DOUBLE) AS calories_kcal,
        CAST(element_at(nutrition_summary, 'タンパク質').value AS DOUBLE) AS protein_g,
        CAST(element_at(nutrition_summary, '脂質').value AS DOUBLE) AS fat_g,
        CAST(element_at(nutrition_summary, '糖質').value AS DOUBLE) AS carbs_g,
        CAST(element_at(nutrition_summary, '食物繊維').value AS DOUBLE) AS fiber_g,
        CAST(element_at(nutrition_summary, '塩分').value AS DOUBLE) AS salt_g,
        CAST(element_at(nutrition_summary, '飽和脂肪酸').value AS DOUBLE) AS saturated_fat_g,
        -- ミネラル
        CAST(element_at(nutrition_summary, 'カリウム').value AS DOUBLE) AS potassium_mg,
        CAST(element_at(nutrition_summary, 'カルシウム').value AS DOUBLE) AS calcium_mg,
        CAST(element_at(nutrition_summary, '鉄').value AS DOUBLE) AS iron_mg,
        -- ビタミン
        CAST(element_at(nutrition_summary, 'ビタミンA').value AS DOUBLE) AS vitamin_a_mcg,
        CAST(element_at(nutrition_summary, 'ビタミンE').value AS DOUBLE) AS vitamin_e_mg,
        CAST(element_at(nutrition_summary, 'ビタミンB1').value AS DOUBLE) AS vitamin_b1_mg,
        CAST(element_at(nutrition_summary, 'ビタミンB2').value AS DOUBLE) AS vitamin_b2_mg,
        CAST(element_at(nutrition_summary, 'ビタミンB6').value AS DOUBLE) AS vitamin_b6_mg,
        CAST(element_at(nutrition_summary, 'ビタミンC').value AS DOUBLE) AS vitamin_c_mg
    FROM base
)

SELECT * FROM extracted
