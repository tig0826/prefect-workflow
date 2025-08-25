--- 各日本語のカラム名を英語に変換する
-- models/staging/stg_price_hourly.sql
{{ config(materialized='view') }}

SELECT
  cast("name" as varchar) as item_name,
  try_cast("価格" as double) as total_amount,
  try_cast("1つあたりの価格" as double) as unit_price,
  try_cast("個数" as integer) as quantity,
  try_cast("できのよさ" as varchar) as quality,
  try_cast(date_parse("出品開始", '%Y/%m/%d') as date) as listing_start_date,
  try_cast(date_parse("出品終了", '%Y/%m/%d') as date) as listing_end_date,
  cast("出品者" as varchar) as seller,
  try_cast("date" as date) as date_raw,
  try_cast("hour" as integer) as hour_raw,
  try_cast("observed_at" as timestamp) as observed_at
from {{ source('dqx', 'price_hourly') }}

