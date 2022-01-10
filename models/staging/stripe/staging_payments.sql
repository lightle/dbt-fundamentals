select id as payment_id,
    status,
    orderid as order_id,
    amount/100 as amount
from raw.stripe.payment
