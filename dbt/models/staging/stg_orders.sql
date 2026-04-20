{# stg_orders.sql #}

{{ config(
    materialized='table'
) }}

with src as (
    select * from {{ source('stg_raw', 'orders') }}
),
deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by order_id
                order by updated_at desc
            ) as rn
        from src
    ) t
    where rn = 1
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

    from deduped
),

valid as (
    select *
    from cleaned
    where order_id is not null
      and customer_id is not null
      and total_amount is not null
      and updated_at is not null
)

select * from valid