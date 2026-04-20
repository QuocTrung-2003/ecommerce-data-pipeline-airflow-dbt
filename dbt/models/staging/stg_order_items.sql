{# stg_order_items.sql #}

{{ config(
    materialized='table'
) }}

with src as (
    select * from {{ source('stg_raw', 'order_items') }}
),
deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by order_item_id
                order by updated_at desc
            ) as rn
        from src
    ) t
    where rn = 1
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

    from deduped
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
)


select * from valid