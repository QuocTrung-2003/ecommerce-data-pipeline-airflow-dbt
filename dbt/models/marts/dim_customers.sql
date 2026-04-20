{{ config(
    materialized='table'
) }}

with c as (
    select * from {{ ref('stg_customers') }}
),

lifecycle as (
    select * from {{ ref('int_customer_lifecycle') }}
)

select
    c.customer_id,
    c.company_name,
    c.country,
    c.industry,
    c.company_size,
    c.signup_month,

    l.customer_status,
    l.customer_segment,
    l.customer_age_days,
    l.revenue,
    l.total_orders

from c
left join lifecycle l
    on c.customer_id = l.customer_id