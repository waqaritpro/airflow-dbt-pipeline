-- Fails if any staged earthquake has a magnitude outside the range the
-- USGS feed is expected to report (-2 to 10). Catches unit/parsing bugs
-- rather than asserting anything about real seismology.
select *
from {{ ref('stg_earthquakes') }}
where magnitude is not null
  and (magnitude < -2 or magnitude > 10)
