with src as (
  select * from raw_data.visits
)

select
  visit_id,
  nullif(customer_id::text,'')::uuid as customer_id,
  lower(source) as source,
  lower(medium) as medium,
  lower(device) as device,
  country,
  pageviews,
  session_duration_s,
  (bounced=1) as bounced,
  (converted=1) as converted,
  visit_start::timestamp as visit_start,
  updated_at::timestamp as updated_at,
  date_trunc('day', visit_start)::date as visit_day
from src