{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

with src as (
    select * from {{ source('stg_raw', 'order_items') }}
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,

        -- type safety
        quantity::int as quantity,
        price::numeric as price,
        total_price::numeric as total_price,

        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from src
),

valid as (
    select *
    from cleaned
    where order_item_id is not null
      and order_id is not null
      and product_id is not null
      and quantity is not null
      and price is not null
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