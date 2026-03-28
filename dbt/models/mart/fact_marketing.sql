with v as (
  select * from {{ ref('int_visits') }}
)

select
  visit_day,
  source,
  medium,
  count(*) as sessions,
  sum(converted::int) as conversions,
  avg((not bounced)::int)::float as engagement_rate
from v
group by 1,2,3