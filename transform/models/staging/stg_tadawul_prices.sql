with source as (

    select * from {{ source('lake', 'tadawul_prices') }}

)

select
    cast(trade_date as date)  as trade_date,
    ticker,
    company,
    sector,
    cast(open as double)      as open_price,
    cast(high as double)      as high_price,
    cast(low as double)       as low_price,
    cast(close as double)     as close_price,
    cast(volume as bigint)    as volume
from source
where close is not null
-- Ingestion re-fetches a 5-day window, so guard against duplicate rows for
-- the same ticker/day ever landing in the lake.
qualify row_number() over (partition by trade_date, ticker order by volume desc) = 1
