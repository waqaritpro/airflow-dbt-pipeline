with staged as (

    select * from {{ ref('stg_earthquakes') }}

)

select
    {{ magnitude_bucket('magnitude') }} as magnitude_bucket,
    count(*) as earthquake_count,
    round(avg(magnitude)::numeric, 2) as avg_magnitude,
    round(min(magnitude)::numeric, 2) as min_magnitude,
    round(max(magnitude)::numeric, 2) as max_magnitude
from staged
group by 1
order by min_magnitude nulls first
