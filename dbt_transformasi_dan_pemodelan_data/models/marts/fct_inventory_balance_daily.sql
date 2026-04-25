select
    product_id,
    store_id,
    date_key,
    qty_in,
    qty_out,
    on_hand_quantity
from {{ ref('int_inventories') }}
