select first_name as customer_first_name,
    last_name as customer_last_name,
    ID as customer_id
from {{ source('jshop', 'customers') }}