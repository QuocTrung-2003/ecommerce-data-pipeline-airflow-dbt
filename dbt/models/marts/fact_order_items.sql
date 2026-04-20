{{ config(
    materialized='table'
) }}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
)

select
    order_item_id,
    order_id,
    product_id,
    product_name,
    category,

    quantity,
    price,
    total_price,

    case
        when order_status = 'delivered' then total_price
        else 0
    end as net_revenue,

    order_day

from items