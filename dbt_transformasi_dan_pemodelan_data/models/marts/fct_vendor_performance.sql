{{ config(materialized='table') }}

WITH purchase_details AS (
    SELECT
        h.vendor_id,
        i.product_id,
        i.store_id,
        h.po_date,
        h.invoice_date,
        h.pay_date,
        i.quantity,
        i.purchase_price,
        i.total_amount,
        i.receiving_date,
        DATEDIFF('day', h.po_date, i.receiving_date) AS lead_time_days
    FROM {{ ref('int_purchases') }} h
    JOIN {{ ref('int_purchase_items') }} i ON h.purchase_id = i.purchase_id
)

SELECT
    vendor_id,
    product_id,
    po_date,
    CAST(strftime(po_date, '%Y%m%d') AS INTEGER) AS date_id,
    SUM(quantity) AS total_quantity_ordered,
    SUM(total_amount) AS total_spend,
    AVG(lead_time_days) AS avg_lead_time_days,
    COUNT(DISTINCT store_id) AS total_stores_supplied
FROM purchase_details
GROUP BY vendor_id, product_id, po_date
