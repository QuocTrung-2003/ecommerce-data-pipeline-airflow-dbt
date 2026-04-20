{# stg_products.sql #}

{{ config(
    materialized='table'
) }}

with src as (
    select * from {{ source('stg_raw', 'products') }}
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id
                order by updated_at desc
            ) as rn
        from src
    ) t
    where rn = 1
),

cleaned as (
    select
        product_id,

        -- text normalization
        trim(product_name) as product_name,
        lower(trim(category)) as category,

        -- numeric safety
        price::numeric as price,

        -- currency standardization
        upper(trim(currency)) as currency,

        updated_at::timestamp as updated_at

    from deduped
),

valid as (
    select *
    from cleaned
    where product_id is not null
      and product_name is not null
      and price is not null
      and updated_at is not null
)

select * from valid