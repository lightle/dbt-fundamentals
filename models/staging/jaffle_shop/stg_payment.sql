select ORDERID as order_id,
    CREATED as payment_date,
    amount,
    status as payment_status
from {{ source('stripe', 'payment') }}