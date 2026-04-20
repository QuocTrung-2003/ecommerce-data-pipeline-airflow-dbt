{{ config(
    materialized='table'
) }}

select
    product_id,
    product_name,
    category,
    price,
    currency

from {{ ref('stg_products') }}