select
    purchase_item_id,
    purchase_id,
    product_id,
    store_id,
    quantity,
    purchase_price,
    total_amount,
    receiving_date
from {{ ref('stg_purchase_items') }}
