{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

with src as (
    select * from {{ source('raw_data', 'products') }}
),

final as (
    select
        product_id,
        product_name,
        category,
        price::numeric as price,
        currency,
        updated_at::timestamp as updated_at
    from src

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final