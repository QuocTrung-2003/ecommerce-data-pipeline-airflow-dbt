{{ config(
    materialized='table'
) }}

with sessions as (
    select * from {{ ref('int_marketing_sessions') }}
)

select
    visit_id,
    visit_day,
    source,
    medium,
    device,

    pageviews,
    session_duration_s,

    bounced,
    converted,

    case
        when converted = true then 1 else 0
    end as is_conversions,

    case
        when bounced = true then 1 else 0
    end as is_bounces

from sessions

{# fact_marketing.sql #}