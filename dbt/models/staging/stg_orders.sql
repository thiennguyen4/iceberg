with source as (
    select * from {{ source('nessie_raw', 'orders') }}
),

cleaned as (
    select
        cast(order_id    as bigint)         as order_id,
        cast(customer_id as bigint)         as customer_id,
        order_time,
        cast(amount      as decimal(10, 2)) as amount,
        lower(trim(status))                 as status,
        created_at
    from source
    where order_id    is not null
      and customer_id is not null
      and amount      > 0
)

select * from cleaned

