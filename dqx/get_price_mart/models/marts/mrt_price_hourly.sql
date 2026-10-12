-- models/marts/mrt_price_hourly.sql
{{ config(
  materialized='incremental',
  unique_key=['item_id','ts_hour'],
  incremental_strategy='merge'
) }}

with

s as (
  select
    cast(item_name as varchar)         as item_id,
    try_cast(unit_price as double)     as unit_price,
    try_cast(quantity as integer)      as quantity,
    observed_at,
    date_trunc('hour', observed_at)    as ts_hour
  from {{ ref('stg_price_hourly') }}
  {% if is_incremental() %}
  -- Read past 48 hours to have window context without using self-referencing subqueries
  where observed_at >= current_timestamp - interval '2' day
  {% endif %}
),

agg as (
  select
    item_id,
    ts_hour,
    count(*)                                             as ticks,
    sum(quantity)                                        as total_qty,
    min(unit_price)                                      as low_raw,
    max(unit_price)                                      as high_raw,
    approx_percentile(unit_price, 0.05)                  as p5_price,
    approx_percentile(unit_price, 0.95)                  as p95_price,
    sum(unit_price * quantity) / nullif(sum(quantity),0) as vwap,
    array_agg(unit_price)                                as price_arr
  from s
  group by item_id, ts_hour
),

base as (
  select
    item_id,
    ts_hour,
    ticks,
    total_qty,
    low_raw,
    high_raw,
    p5_price,
    p95_price,
    reduce(
      filter(price_arr, x -> x between p5_price and p95_price),
      cast(row(0.0, 0) as row(s double, c integer)),
      (acc, x) -> cast(row(acc.s + x, acc.c + 1) as row(s double, c integer)),
      acc -> if(acc.c = 0, null, acc.s / acc.c)
    )                                                    as trimmed_mean_5_95,
    vwap,
    coalesce(p5_price, vwap, (low_raw + high_raw)/2)    as price_core
  from agg
),

-- lag-based columns (log return, gain, loss) — one window sort pass
with_lag as (
  select
    b.*,
    ln(
      b.price_core
      / nullif(lag(b.price_core) over (partition by b.item_id order by b.ts_hour), 0)
    ) as log_return_1h,
    greatest(b.price_core - lag(b.price_core) over (partition by b.item_id order by b.ts_hour), 0) as gain_1h,
    greatest(lag(b.price_core) over (partition by b.item_id order by b.ts_hour) - b.price_core, 0) as loss_1h
  from base b
),

-- all rolling aggregates in ONE CTE so Trino shares the partition/sort pass
-- (replaces the original tech1/tech2/tech3/tech4 chain)
tech as (
  select
    *,
    avg(price_core) over (
      partition by item_id order by ts_hour
      rows between 23 preceding and current row
    ) as ma_24h,
    stddev_samp(log_return_1h) over (
      partition by item_id order by ts_hour
      rows between 23 preceding and current row
    ) as vol_24h,
    avg(gain_1h) over (
      partition by item_id order by ts_hour
      rows between 13 preceding and current row
    ) as avg_gain_14,
    avg(loss_1h) over (
      partition by item_id order by ts_hour
      rows between 13 preceding and current row
    ) as avg_loss_14,
    avg(price_core) over (
      partition by item_id order by ts_hour
      rows between 11 preceding and current row
    ) as sma_12,
    avg(price_core) over (
      partition by item_id order by ts_hour
      rows between 25 preceding and current row
    ) as sma_26
  from with_lag
),

final as (
  select
    item_id,
    ts_hour,
    ticks,
    total_qty,
    low_raw,
    high_raw,
    p5_price,
    p95_price,
    trimmed_mean_5_95,
    vwap,
    ma_24h,
    vol_24h,
    log_return_1h,
    case
      when coalesce(avg_loss_14, 0) = 0 then null
      else 100 - 100 / (1 + avg_gain_14 / nullif(avg_loss_14, 0))
    end as rsi_14_sma,
    sma_12 - sma_26 as macd_line_sma,
    avg(sma_12 - sma_26) over (
      partition by item_id order by ts_hour
      rows between 8 preceding and current row
    ) as macd_signal_sma,
    (sma_12 - sma_26) - avg(sma_12 - sma_26) over (
      partition by item_id order by ts_hour
      rows between 8 preceding and current row
    ) as macd_hist_sma,
    current_timestamp as ingested_at
  from tech
)

select * from final
{% if is_incremental() %}
-- Only MERGE new hours; historical rows are immutable price data
where ts_hour > (select coalesce(max(ts_hour), timestamp '1970-01-01 00:00:00') from {{ this }})
{% endif %}
