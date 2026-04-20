{# stg_visits.sql #}

{{ config(
    materialized='table'
) }}

with src as (
    select * from {{ source('stg_raw', 'visits') }}
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by visit_id
                order by updated_at desc
            ) as rn
        from src
    ) t
    where rn = 1
),

cleaned as (
    select
        visit_id,

        -- FK safety
        nullif(customer_id::text, '')::uuid as customer_id,

        -- normalize marketing attributes
        lower(trim(source)) as source,
        lower(trim(medium)) as medium,
        lower(trim(device)) as device,

        upper(trim(country)) as country,

        -- metrics
        coalesce(pageviews, 0)::int as pageviews,
        coalesce(session_duration_s, 0)::int as session_duration_s,

        -- boolean normalization
        case when bounced = 1 then true else false end as bounced,
        case when converted = 1 then true else false end as converted,

        visit_start::timestamp as visit_start,
        updated_at::timestamp as updated_at,

        date_trunc('day', visit_start)::date as visit_day

    from deduped
),

valid as (
    select *
    from cleaned
    where visit_id is not null
      and updated_at is not null
)

select * from valid