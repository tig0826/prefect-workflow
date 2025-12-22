-- models/marts/mrt_price_daily.sql
{{ config(
  materialized='incremental',
  unique_key=['item_id','ts_day'],
  incremental_strategy='merge'
) }}

with src as (
  select
    item_id,
    date_trunc('day', ts_hour) as ts_day,
    ticks,
    total_qty,
    p5_price,
    p95_price,
    vwap,
    low_raw,
    high_raw
  from {{ ref('mrt_price_hourly') }}
  {% if is_incremental() %}
    where ts_hour >= (current_date - interval '90' day)
  {% endif %}
)
select
  item_id,
  ts_day,
  count(*)                           as hours_observed,
  sum(ticks)                         as ticks_day,
  sum(total_qty)                     as total_qty_day,
  min(low_raw)                       as low_raw_day,
  max(high_raw)                      as high_raw_day,
  approx_percentile(p5_price, 0.5)   as p5_day,
  approx_percentile(p95_price, 0.5)  as p95_day,
  avg(vwap)                          as vwap_day
from src
group by 1,2
