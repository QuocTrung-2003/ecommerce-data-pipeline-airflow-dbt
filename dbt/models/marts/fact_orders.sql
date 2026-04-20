{{ config(
    materialized='table'
) }}

with orders as (
    select * from {{ ref('int_customer_orders') }}
)

select
    order_id,
    customer_id,
    order_day,
    status,

    -- KPI CORE
    case
        when status = 'delivered' then total_amount
        else 0
    end as revenue,

    total_amount,
    payment_method,

    -- business flags
    case when status = 'cancelled' then 1 else 0 end as is_cancelled,
    case when status = 'returned' then 1 else 0 end as is_returned

from orders