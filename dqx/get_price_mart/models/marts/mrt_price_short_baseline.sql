-- models/marts/mrt_price_short_baseline.sql
{{ config(
  materialized='incremental',
  unique_key=['item_id','ts_day'],
  incremental_strategy='merge'
) }}

with d as (
  select * from {{ ref('mrt_price_daily') }}
  {% if is_incremental() %}
    where ts_day >= (current_date - interval '90' day)
  {% endif %}
),
w as (
  select
    item_id,
    ts_day,
    p5_day,
    vwap_day,
    avg(p5_day) over (
      partition by item_id
      order by ts_day
      rows between 6 preceding and current row
    ) as ma7_p5,
    avg(p5_day) over (
      partition by item_id
      order by ts_day
      rows between 29 preceding and current row
    ) as ma30_p5,
    stddev_samp(p5_day) over (
      partition by item_id
      order by ts_day
      rows between 29 preceding and current row
    ) as sd30_p5
  from d
)
select
  item_id,
  ts_day,
  p5_day,
  vwap_day,
  ma7_p5,
  ma30_p5,
  sd30_p5,
  case
    when coalesce(sd30_p5,0) = 0 then null
    else (p5_day - ma30_p5) / sd30_p5
  end as z30_p5
from w
