with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

agg as (
    select
        customer_id,
        count(*) as total_orders,
        sum(case when status = 'delivered' then total_amount else 0 end) as revenue,
        max(created_at) as last_order_date
    from orders
    group by 1
)

select
    c.customer_id,
    c.company_name,
    c.country,
    c.company_size,
    c.industry,
    c.signup_date,

    coalesce(a.total_orders, 0) as total_orders,
    coalesce(a.revenue, 0) as revenue,
    a.last_order_date,


    case
        when c.company_size = '500+' then 'enterprise'
        when c.company_size in ('201-500','51-200') then 'mid_market'
        else 'small_business'
    end as customer_segment,

    current_date - c.signup_date as customer_age_days,

    -- status logic
    case
        when c.is_churned then 'churned'
        when coalesce(a.total_orders,0) = 0 then 'inactive'
        else 'active'
    end as customer_status

from customers c
left join agg a
    on c.customer_id = a.customer_id