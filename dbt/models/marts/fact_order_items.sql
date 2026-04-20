with oi as (
  select * from {{ ref('int_order_items') }}
)

select
  order_item_id,
  order_id,
  product_id,
  quantity,
  price,
  total_price
from oi