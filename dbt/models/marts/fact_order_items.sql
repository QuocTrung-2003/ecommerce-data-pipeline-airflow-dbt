{{ config(
    materialized='table'
) }}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
)

select
    order_item_id,
    order_id,
    product_id,
    product_name,
    category,

    quantity,
    price,
    total_price,

    order_status,
    order_day

from items
{# fact_order_items.sql #}