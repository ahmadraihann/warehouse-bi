{{ config(materialized='view') }}

select
    purchase_item_id,
    purchase_id,
    product_id,
    store_id,
    quantity,
    purchase_price,
    total_amount,
    receiving_date
from {{ source('inventory', 'purchase_items') }}
