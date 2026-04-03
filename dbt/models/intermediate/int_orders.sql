{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with src as (
    select * from {{ source('raw_data', 'orders') }}
),

final as (
    select
        order_id,
        customer_id,
        total_amount::numeric as total_amount,
        status,
        payment_method,
        country,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        date_trunc('day', created_at)::date as order_day
    from src

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final