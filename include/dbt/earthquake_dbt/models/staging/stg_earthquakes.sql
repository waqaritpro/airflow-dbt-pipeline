with source as (

    select * from {{ source('raw', 'earthquakes') }}

),

cleaned as (

    select
        id as earthquake_id,
        event_time,
        (event_time at time zone 'UTC')::date as event_date,
        mag::double precision as magnitude,
        trim(place) as place,

        -- USGS place strings are typically "<distance/direction> of <region>";
        -- take the text after the last comma as the region when present,
        -- otherwise fall back to the full place string.
        case
            when position(',' in place) > 0
                then trim(split_part(
                    place, ',', array_length(string_to_array(place, ','), 1)
                ))
            else trim(place)
        end as region,

        longitude::double precision as longitude,
        latitude::double precision as latitude,
        depth_km::double precision as depth_km,
        ingested_at
    from source
    where event_time is not null

)

select
    earthquake_id,
    event_time,
    event_date,
    magnitude,
    place,
    nullif(region, '') as region,
    longitude,
    latitude,
    depth_km,
    ingested_at
from cleaned
