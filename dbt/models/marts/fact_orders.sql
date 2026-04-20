with o as (
  select * from {{ ref('int_orders') }}
)

select
  order_id,
  customer_id,
  total_amount,
  status,
  payment_method,
  country,
  order_day
from o