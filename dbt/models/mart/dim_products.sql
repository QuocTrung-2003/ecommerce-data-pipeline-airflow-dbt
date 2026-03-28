with p as (
  select * from {{ ref('int_products') }}
)

select
  product_id,
  product_name,
  category,
  price,
  currency
from p