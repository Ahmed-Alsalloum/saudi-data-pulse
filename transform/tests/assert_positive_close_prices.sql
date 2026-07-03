-- Data-quality gate: a non-positive close price means the feed is corrupt;
-- fail the build before it reaches the marts.
select *
from {{ ref('stg_tadawul_prices') }}
where close_price <= 0
