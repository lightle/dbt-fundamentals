with customers as (
    select * from {{ ref('staging_customers') }}
),

payments as (
    select order_id,
        sum(amount) as amount

    from {{ ref('staging_payments') }}
    where status = 'success'
    group by order_id 
),

orders as (
    select * from {{ ref('staging_orders') }}
),

order_payment as (
    select orders.customer_id,
        orders.order_date,
        orders.order_id,
        coalesce(payments.amount, 0) as amount
        
        from orders
        left join payments using (order_id)
),

customer_orders as (
    select customer_id,
    max(order_date) as last_order_date,
    min(order_date) as first_order_date,
    count(order_id) as number_of_orders,
    sum(amount) as lifetime_value

    from order_payment
    group by customer_id
),

final as (
    select customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.last_order_date,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

        from customers
        left join customer_orders using (customer_id)

)

select * from final