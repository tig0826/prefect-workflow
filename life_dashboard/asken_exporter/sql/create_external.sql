CREATE TABLE IF NOT EXISTS hive.life.asken_external (
    "date" VARCHAR,
    meal_records ROW(
        breakfast ROW(total_kcal DOUBLE, items ARRAY(ROW(name VARCHAR, quantity VARCHAR, kcal DOUBLE))),
        lunch ROW(total_kcal DOUBLE, items ARRAY(ROW(name VARCHAR, quantity VARCHAR, kcal DOUBLE))),
        dinner ROW(total_kcal DOUBLE, items ARRAY(ROW(name VARCHAR, quantity VARCHAR, kcal DOUBLE))),
        sweets ROW(total_kcal DOUBLE, items ARRAY(ROW(name VARCHAR, quantity VARCHAR, kcal DOUBLE))),
        exercise ROW(total_kcal DOUBLE, items ARRAY(ROW(name VARCHAR, quantity VARCHAR, kcal DOUBLE)))
    ),
    nutrition_summary MAP(VARCHAR, ROW("value" DOUBLE, unit VARCHAR)),
    dt VARCHAR
) WITH (
    format = 'JSON',
    external_location = 's3a://bronze-zone/asken/raw/',
    partitioned_by = ARRAY['dt']
);
