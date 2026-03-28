with src as (
  select * from raw_data.order_items
)

select
  order_item_id,
  order_id,
  product_id,
  quantity::int as quantity,
  price::numeric as price,
  total_price::numeric as total_price,
  created_at::timestamp as created_at,
  updated_at::timestamp as updated_at
from src