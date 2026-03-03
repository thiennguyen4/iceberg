with orders as (
    select * from {{ ref('stg_orders') }}
),

daily_agg as (
    select
        date(order_timestamp)               as order_date,
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as unique_customers,
        sum(price * quantity)               as total_revenue,
        round(avg(price * quantity), 2)     as avg_order_value,
        min(price * quantity)               as min_order_value,
        max(price * quantity)               as max_order_value,
        current_timestamp()                 as updated_at
    from orders
    group by date(order_timestamp)
)

select * from daily_agg

