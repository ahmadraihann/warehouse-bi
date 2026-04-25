select
    store_id,
    city
from {{ source('inventory', 'stores') }}
