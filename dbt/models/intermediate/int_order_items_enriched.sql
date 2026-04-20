with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    i.order_item_id,
    i.order_id,
    i.product_id,

    p.product_name,
    p.category,

    o.customer_id,
    o.order_day,
    o.status as order_status,

    i.quantity,
    i.price,
    i.total_price

from items i
left join products p
    on i.product_id = p.product_id
left join orders o
    on i.order_id = o.order_id