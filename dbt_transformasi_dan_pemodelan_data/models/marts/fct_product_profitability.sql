{{ config(materialized='table') }}

WITH avg_purchase_prices AS (
    SELECT
        product_id,
        AVG(purchase_price) AS avg_unit_cost
    FROM {{ ref('stg_purchase_items') }}
    GROUP BY product_id
),

sales_data AS (
    SELECT
        product_id,
        store_id,
        SUM(quantity) AS total_qty_sold,
        SUM(total_amount - excise_tax) AS net_revenue
    FROM {{ ref('stg_sales') }}
    GROUP BY product_id, store_id
)

SELECT
    s.product_id,
    s.store_id,
    s.total_qty_sold,
    s.net_revenue,
    (s.total_qty_sold * COALESCE(p.avg_unit_cost, 0)) AS total_estimated_cost,
    (s.net_revenue - (s.total_qty_sold * COALESCE(p.avg_unit_cost, 0))) AS estimated_profit,
    CASE 
        WHEN s.net_revenue > 0 
        THEN (s.net_revenue - (s.total_qty_sold * COALESCE(p.avg_unit_cost, 0))) / s.net_revenue 
        ELSE 0 
    END AS profit_margin
FROM sales_data s
LEFT JOIN avg_purchase_prices p ON s.product_id = p.product_id
