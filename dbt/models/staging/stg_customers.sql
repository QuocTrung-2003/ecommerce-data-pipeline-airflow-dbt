{# stg_customers.sql #}

{{ config(
    materialized='table'
) }}

with src as (
    select * from {{ source('stg_raw', 'customers') }}
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by customer_id
                order by updated_at desc
            ) as rn
        from src
    ) t
    where rn = 1
),

cleaned as (
    select
        customer_id,
        trim(company_name) as company_name,
        upper(trim(country)) as country,
        trim(industry) as industry,
        trim(company_size) as company_size,
        signup_date::timestamp as signup_date,
        updated_at::timestamp as updated_at,
        is_churned::boolean as is_churned,
        date_trunc('month', signup_date)::date as signup_month
    from deduped
),

valid as (
    select *
    from cleaned
    where customer_id is not null
      and company_name is not null
      and updated_at is not null
      and signup_date is not null
)

select * from valid