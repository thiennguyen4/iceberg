
with completed_orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'completed'
),

daily_agg as (
    select
        date(order_time)            as order_date,
        count(distinct order_id)    as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(amount)                 as total_revenue,
        round(avg(amount), 2)       as avg_order_value,
        min(amount)                 as min_order_value,
        max(amount)                 as max_order_value,
        current_timestamp()         as updated_at
    from completed_orders
    group by date(order_time)
)

select * from daily_agg

