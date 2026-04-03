{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

with src as (
    select * from {{ source('raw_data', 'order_items') }}
),

final as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity::int as quantity,
        price::numeric as price,
        total_price::numeric as total_price,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at
    from src

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final