with payment as (
    select * from {{ ref ('stg_payment') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_payment as (
    select order_id,
        max(payment_date) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from payment
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name
    FROM orders
left join order_payment using (order_id)
left join customers using (customer_id)
),

customer_orders as (
    select customer_id,
        min(order_placed_at) as first_order_date,
        max(order_placed_at) as most_recent_order_date,
        count(order_id) AS number_of_orders
    from orders
    group by 1
),

lifetime_value as (
    select order_id,
        sum(total_amount_paid) over (partition by customer_id order by order_id asc) as customer_lifetime_value
    from paid_orders
),

final as (
    select order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        ROW_NUMBER() OVER (ORDER BY order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_id) as customer_sales_seq,
        CASE 
            WHEN first_order_date = order_placed_at
            THEN 'new'
            ELSE 'return'
            END as nvsr,
        customer_lifetime_value,
        first_order_date as fdos
    FROM paid_orders p
    left join customer_orders as c USING (customer_id)
    left outer join lifetime_value using (order_id)
    ORDER BY order_id
)

select * from final