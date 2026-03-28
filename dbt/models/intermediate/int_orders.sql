with src as (
  select * from raw_data.orders
)

select
  order_id,
  customer_id,
  total_amount::numeric as total_amount,
  status,
  payment_method,
  country,
  created_at::timestamp as created_at,
  updated_at::timestamp as updated_at,
  date_trunc('day', created_at)::date as order_day
from src