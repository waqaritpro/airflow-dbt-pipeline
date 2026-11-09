with staged as (

    select * from {{ ref('stg_earthquakes') }}

)

select
    event_date,
    count(*) as earthquake_count,
    round(avg(magnitude)::numeric, 2) as avg_magnitude,
    round(min(magnitude)::numeric, 2) as min_magnitude,
    round(max(magnitude)::numeric, 2) as max_magnitude,
    count(*) filter (where magnitude >= 4) as significant_earthquake_count
from staged
group by event_date
order by event_date
