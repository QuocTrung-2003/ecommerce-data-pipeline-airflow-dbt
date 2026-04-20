{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

with src as (
    select * from {{ source('stg_raw', 'products') }}
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

    from src
),

valid as (
    select *
    from cleaned
    where product_id is not null
      and product_name is not null
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