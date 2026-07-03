select
    cast(observed_at as date)        as obs_date,
    city,
    round(min(temperature_c), 1)     as min_temperature_c,
    round(max(temperature_c), 1)     as max_temperature_c,
    round(avg(temperature_c), 1)     as avg_temperature_c,
    round(avg(humidity_pct), 1)      as avg_humidity_pct,
    round(max(wind_speed_kmh), 1)    as max_wind_speed_kmh,
    count(*)                         as hours_reported
from {{ ref('stg_weather') }}
group by obs_date, city
order by obs_date desc, city
