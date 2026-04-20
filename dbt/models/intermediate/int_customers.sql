{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with src as (
    select * from {{ source('stg_raw', 'customers') }}
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
    from src
),

valid as (
    select *
    from cleaned
    where customer_id is not null
      and company_name is not null
      and updated_at is not null
      and signup_date is not null
),

final as (
    select *
    from valid

    {% if is_incremental() %}
    where updated_at > (
        select coalesce(max(updated_at), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)

select * from final