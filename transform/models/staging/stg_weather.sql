with source as (

    select * from {{ source('lake', 'weather_hourly') }}

)

select
    cast(observed_at as timestamp)  as observed_at,
    city,
    cast(temperature_c as double)   as temperature_c,
    cast(humidity_pct as double)    as humidity_pct,
    cast(wind_speed_kmh as double)  as wind_speed_kmh
from source
where temperature_c is not null
-- Hourly runs re-fetch a 48h window, so the same observation lands many times.
qualify row_number() over (partition by city, observed_at order by observed_at) = 1
