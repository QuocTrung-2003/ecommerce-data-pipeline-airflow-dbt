with visits as (
    select * from {{ ref('stg_visits') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    v.visit_id,
    v.customer_id,

    c.company_size,
    c.industry,

    v.source,
    v.medium,
    v.device,
    v.country,

    v.pageviews,
    v.session_duration_s,
    v.bounced,
    v.converted,

    v.visit_day

from visits v
left join customers c
    on v.customer_id = c.customer_id