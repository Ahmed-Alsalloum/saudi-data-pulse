with prices as (

    select * from {{ ref('stg_tadawul_prices') }}

),

with_prev as (

    select
        *,
        lag(close_price) over (partition by ticker order by trade_date) as prev_close
    from prices

),

daily as (

    select
        *,
        (close_price - prev_close) / prev_close * 100 as change_pct
    from with_prev
    where prev_close is not null and prev_close > 0

)

select
    trade_date,
    sector,
    count(distinct ticker)                    as tickers,
    sum(volume)                               as total_volume,
    round(avg(change_pct), 2)                 as avg_change_pct,
    arg_max(company, change_pct)              as top_gainer,
    round(max(change_pct), 2)                 as top_gainer_change_pct,
    arg_min(company, change_pct)              as top_loser,
    round(min(change_pct), 2)                 as top_loser_change_pct
from daily
group by trade_date, sector
order by trade_date desc, sector
