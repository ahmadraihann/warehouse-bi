{{ config(materialized='view') }}

select
    purchase_id,
    vendor_id,
    po_number,
    po_date,
    invoice_date,
    pay_date
from {{ source('inventory', 'purchases') }}
