-- models/marts/mrt_price_short_baseline.sql
{{ config(
  materialized='incremental',
  unique_key=['item_id','ts_day'],
  incremental_strategy='merge'
) }}

with

{% if is_incremental() %}
_bounds as (
  select coalesce(max(ts_day), date '1970-01-01') as last_day
  from {{ this }}
),
{% endif %}

d as (
  select * from {{ ref('mrt_price_daily') }}
  {% if is_incremental() %}
    -- 32 days: 31 for MA30 context + 1 new (was 35 — tightened to cut MERGE size)
    where ts_day >= (select date_add('day', -32, last_day) from _bounds)
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
    when coalesce(sd30_p5, 0) = 0 then null
    else (p5_day - ma30_p5) / sd30_p5
  end as z30_p5
from w
{% if is_incremental() %}
-- Only MERGE the new day; historical baseline values don't change
where ts_day > (select last_day from _bounds)
{% endif %}
