{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='meal_date',
    table_type='iceberg',
    format='parquet'
) }}

WITH raw_asken AS (
    SELECT
        CAST(dt AS DATE) AS meal_date,
        nutrition_summary
    FROM {{ source('hive_life_bronze', 'asken_external') }}
    {% if is_incremental() %}
    -- current_date は Trino のセッションTZ（=UTC）基準なので、JST の早朝に
    -- 1日ずれて当日を取り逃す（JST 08:00 = UTC 前日23:00）。JST で明示する。
    -- 窓を 3日→14日に広げているのは、あすけんは後から入力を編集するため
    -- （実際に夕食を削除しても silver が 1018kcal のまま古い値を保持していた）。
    WHERE CAST(dt AS DATE) >= date_add('day', -14,
        CAST(current_timestamp AT TIME ZONE 'Asia/Tokyo' AS DATE))
    {% endif %}
),

extracted AS (
    SELECT
        meal_date,
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
    FROM raw_asken
)

SELECT * FROM extracted
