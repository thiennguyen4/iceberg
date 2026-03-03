with source as (
    select * from {{ source('hive_raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        trim(product_name)                  as product_name,
        quantity,
        cast(price as decimal(10, 2))       as price,
        order_timestamp
    from source
    where order_id      is not null
      and customer_id   is not null
      and quantity      > 0
      and price         > 0
)

select * from cleaned

