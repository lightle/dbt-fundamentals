select id as payment_id,
    status,
    orderid as order_id,
    amount
from raw.stripe.payment
