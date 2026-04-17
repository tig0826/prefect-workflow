WITH meal_agg AS (
    SELECT
        dt,
        meal_type,
        ARRAY_JOIN(ARRAY_AGG(menu_name), '||') AS items_str,
        SUM(calories_kcal) AS total_calories
    FROM {{ ref('asken_meal') }}
    GROUP BY dt, meal_type
),

meal_pivot AS (
    SELECT
        dt,
        -- 文字列としてピボットする
        MAX(IF(meal_type = '朝食', items_str)) AS breakfast_items,
        SUM(IF(meal_type = '朝食', total_calories)) AS breakfast_calories,
        MAX(IF(meal_type = '昼食', items_str)) AS lunch_items,
        SUM(IF(meal_type = '昼食', total_calories)) AS lunch_calories,
        MAX(IF(meal_type = '夕食', items_str)) AS dinner_items,
        SUM(IF(meal_type = '夕食', total_calories)) AS dinner_calories,
        MAX(IF(meal_type = '間食', items_str)) AS snack_items,
        SUM(IF(meal_type = '間食', total_calories)) AS snack_calories
    FROM meal_agg
    GROUP BY dt
),

nutrition AS (
    SELECT * FROM {{ ref('asken_nutrition') }}
)

SELECT
    n.dt AS target_date,
    -- 文字列型なので、空の場合は空文字列 '' と結合させる
    COALESCE(m.breakfast_items, '') AS breakfast_items,
    COALESCE(m.breakfast_calories, 0) AS breakfast_calories,
    COALESCE(m.lunch_items, '') AS lunch_items,
    COALESCE(m.lunch_calories, 0) AS lunch_calories,
    COALESCE(m.dinner_items, '') AS dinner_items,
    COALESCE(m.dinner_calories, 0) AS dinner_calories,
    COALESCE(m.snack_items, '') AS snack_items,
    COALESCE(m.snack_calories, 0) AS snack_calories,
    -- 栄養素データ（以下略）
    n.calories_kcal, n.protein_g, n.fat_g, n.carbs_g, n.fiber_g, n.salt_g, 
    n.saturated_fat_g, n.potassium_mg, n.calcium_mg, n.iron_mg, 
    n.vitamin_a_mcg, n.vitamin_e_mg, n.vitamin_b1_mg, n.vitamin_b2_mg, 
    n.vitamin_b6_mg, n.vitamin_c_mg
FROM nutrition n
LEFT JOIN meal_pivot m ON n.dt = m.dt
