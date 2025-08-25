-- models/marts/mrt_price_hourly.sql
{{ config(
  materialized='incremental',
  unique_key=['item_id','ts_hour'],
  incremental_strategy='merge'
) }}

with s as (
  -- stg直結。必要十分な列だけ選ぶ
  select
    cast(item_name as varchar)         as item_id,
    try_cast(unit_price as double)     as unit_price,
    try_cast(quantity as integer)      as quantity,
    try_cast(observed_at as timestamp) as observed_at,
    date_trunc('hour', observed_at)    as ts_hour
  from {{ ref('stg_price_hourly') }}
  {% if is_incremental() %}
    where observed_at >= date_add('day', -14, current_date)
  {% endif %}
),

-- 1時間の基本統計＋百分位とVWAP
ptiles as (
  select
    item_id,
    ts_hour,
    count(*)                                           as ticks,
    sum(quantity)                                      as total_qty,
    min(unit_price)                                    as low_raw,
    max(unit_price)                                    as high_raw,
    approx_percentile(unit_price, 0.05)                as p5_price,
    approx_percentile(unit_price, 0.95)                as p95_price,
    sum(unit_price * quantity) / nullif(sum(quantity),0) as vwap
  from s
  group by item_id, ts_hour
),

-- トリム平均（5%～95%の間だけ平均）
trimmed as (
  select
    p.item_id,
    p.ts_hour,
    max(p.ticks)        as ticks,
    max(p.total_qty)    as total_qty,
    max(p.low_raw)      as low_raw,
    max(p.high_raw)     as high_raw,
    max(p.p5_price)     as p5_price,
    max(p.p95_price)    as p95_price,
    max(p.vwap)         as vwap,
    avg(s.unit_price) filter (
      where s.unit_price between p.p5_price and p.p95_price
    )                  as trimmed_mean_5_95
  from ptiles p
  left join s
    on s.item_id = p.item_id and s.ts_hour = p.ts_hour
  group by p.item_id, p.ts_hour
),

-- ローリングの“基準価格”を一意に決める（P5優先→VWAP→(low+high)/2）
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
    trimmed_mean_5_95,
    vwap,
    coalesce(p5_price, vwap, (low_raw + high_raw)/2) as price_core
  from trimmed
),

-- 連続対数リターン r_t = ln(price_t / price_{t-1})
with_ret as (
  select
    b.*,
    ln(
      b.price_core
      / nullif(lag(b.price_core) over (partition by b.item_id order by b.ts_hour), 0)
    ) as log_return_1h
  from base b
),

-- SMAベースのテクニカル（EMAの簡易近似）
tech1 as (
  select
    *,
    -- 24h移動平均（価格の平滑）
    avg(price_core) over (
      partition by item_id
      order by ts_hour
      rows between 23 preceding and current row
    ) as ma_24h,

    -- 24hボラティリティ（対数リターンのstddev）
    stddev_samp(log_return_1h) over (
      partition by item_id
      order by ts_hour
      rows between 23 preceding and current row
    ) as vol_24h,

    -- RSI（SMA14近似）：上げ幅/下げ幅の平均
    greatest(price_core - lag(price_core) over (partition by item_id order by ts_hour), 0) as gain_1h,
    greatest(lag(price_core) over (partition by item_id order by ts_hour) - price_core, 0) as loss_1h
  from with_ret
),

tech2 as (
  select
    *,
    avg(gain_1h) over (
      partition by item_id
      order by ts_hour
      rows between 13 preceding and current row
    ) as avg_gain_14,
    avg(loss_1h) over (
      partition by item_id
      order by ts_hour
      rows between 13 preceding and current row
    ) as avg_loss_14
  from tech1
),

tech3 as (
  select
    *,
    case
      when coalesce(avg_loss_14,0) = 0 then null
      else 100 - 100 / (1 + (avg_gain_14 / nullif(avg_loss_14,0)))
    end as rsi_14_sma,

    -- MACD（SMA近似）：短期SMA12 - 長期SMA26
    avg(price_core) over (
      partition by item_id
      order by ts_hour
      rows between 11 preceding and current row
    ) as sma_12,
    avg(price_core) over (
      partition by item_id
      order by ts_hour
      rows between 25 preceding and current row
    ) as sma_26
  from tech2
),

tech4 as (
  select
    *,
    (sma_12 - sma_26) as macd_line_sma,
    -- シグナルはMACDラインのSMA9
    avg(sma_12 - sma_26) over (
      partition by item_id
      order by ts_hour
      rows between 8 preceding and current row
    ) as macd_signal_sma
  from tech3
),

final as (
  select
    item_id,
    ts_hour,
    ticks,
    total_qty,
    -- 生の極値（外れ値確認用）
    low_raw,
    high_raw,
    -- トリム・代表値群
    p5_price,
    p95_price,
    trimmed_mean_5_95,
    vwap,
    -- トレンド/ゆらぎ/テクニカル
    ma_24h,
    vol_24h,
    log_return_1h,
    rsi_14_sma,
    macd_line_sma,
    macd_signal_sma,
    (macd_line_sma - macd_signal_sma) as macd_hist_sma,
    current_timestamp as ingested_at
  from tech4
)

select * from final
