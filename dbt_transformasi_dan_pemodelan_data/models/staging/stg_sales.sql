{{ config(materialized='view') }}

select
    sales_id,
    store_id,
    product_id,
    sales_date,
    quantity,
    sales_price,
    total_amount,
    excise_tax
from {{ source('inventory', 'sales') }}
