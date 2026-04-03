{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with src as (
    select * from {{ source('raw_data', 'customers') }}
),

final as (
    select
        customer_id,
        company_name,
        upper(country) as country,
        industry,
        company_size,
        signup_date::timestamp as signup_date,
        updated_at::timestamp as updated_at,
        is_churned::boolean as is_churned,
        date_trunc('month', signup_date)::date as signup_month
    from src

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final