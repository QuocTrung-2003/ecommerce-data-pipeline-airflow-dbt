with src as (
  select * from raw_data.products
)

select
  product_id,
  product_name,
  category,
  price::numeric as price,
  currency,
  updated_at::timestamp as updated_at
from src