{{ config(materialized='table') }}

SELECT
    product_id,
    store_id,
    sales_date,
    CAST(strftime(sales_date, '%Y%m%d') AS INTEGER) AS date_key,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS gross_revenue,
    SUM(excise_tax) AS total_tax,
    SUM(total_amount - excise_tax) AS net_revenue
FROM {{ ref('stg_sales') }}
GROUP BY product_id, store_id, sales_date
