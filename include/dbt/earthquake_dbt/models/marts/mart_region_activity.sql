with staged as (

    select * from {{ ref('stg_earthquakes') }}

)

select
    region,
    count(*) as earthquake_count,
    round(avg(magnitude)::numeric, 2) as avg_magnitude,
    round(max(magnitude)::numeric, 2) as max_magnitude,
    min(event_date) as first_event_date,
    max(event_date) as last_event_date
from staged
where region is not null
group by region
order by earthquake_count desc
