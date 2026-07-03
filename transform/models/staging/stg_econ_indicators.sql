with source as (

    select * from {{ source('lake', 'econ_indicators') }}

)

select
    indicator_code,
    indicator_name,
    cast(year as integer)  as year,
    cast(value as double)  as value
from source
where value is not null
qualify row_number() over (partition by indicator_code, year order by year) = 1
