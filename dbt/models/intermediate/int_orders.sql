{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with src as (
    select * from {{ source('stg_raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,

        -- numeric safety
        total_amount::numeric as total_amount,

        -- normalize text
        lower(trim(status)) as status,
        lower(trim(payment_method)) as payment_method,
        upper(trim(country)) as country,

        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,

        -- derived column
        date_trunc('day', created_at)::date as order_day

    from src
),

valid as (
    select *
    from cleaned
    where order_id is not null
      and customer_id is not null
      and total_amount is not null
      and updated_at is not null
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