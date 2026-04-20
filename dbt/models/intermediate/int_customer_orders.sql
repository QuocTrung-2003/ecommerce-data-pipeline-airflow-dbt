with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.company_name,
    c.country as customer_country,
    c.company_size,
    c.industry,

    o.total_amount,
    o.status,
    o.payment_method,
    o.order_day,
    o.created_at

from orders o
left join customers c
    on o.customer_id = c.customer_id