with orders as (
    select order_id, customer_id from {{ ref('staging_orders') }}
),

payments as (
    select order_id,
        sum(amount) as amount

    from {{ ref('staging_payments') }}
    group by order_id 
),

final as (
    select orders.customer_id,
        orders.order_id,
        coalesce(payments.amount, 0) as amount
    
    from orders
    left join payments using (order_id)
)

select * from final