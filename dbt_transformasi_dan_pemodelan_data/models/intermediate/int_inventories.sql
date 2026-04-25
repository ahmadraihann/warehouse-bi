{{ config(materialized='table') }}

WITH purchases AS (
    SELECT
        product_id,
        store_id,
        receiving_date AS date,
        SUM(quantity) AS qty_in
    FROM {{ ref('stg_purchase_items') }}
    GROUP BY product_id, store_id, receiving_date
),

sales AS (
    SELECT
        product_id,
        store_id,
        sales_date AS date,
        SUM(quantity) AS qty_out
    FROM {{ ref('stg_sales') }}
    GROUP BY product_id, store_id, sales_date
),

all_dates AS (
    SELECT date FROM (
        SELECT receiving_date AS date FROM {{ ref('stg_purchase_items') }}
        UNION
        SELECT sales_date FROM {{ ref('stg_sales') }}
    )
),

combined AS (

    SELECT
        COALESCE(p.product_id, s.product_id) AS product_id,
        COALESCE(p.store_id, s.store_id) AS store_id,
        COALESCE(p.date, s.date) AS date,

        COALESCE(qty_in, 0) AS qty_in,
        COALESCE(qty_out, 0) AS qty_out

    FROM purchases p
    FULL OUTER JOIN sales s
        ON p.product_id = s.product_id
        AND p.store_id = s.store_id
        AND p.date = s.date

),

final AS (

    SELECT
        product_id,
        store_id,
        CAST(strftime(date, '%Y%m%d') AS INTEGER) AS date_key,

        qty_in,
        qty_out,

        -- cumulative inventory (THIS IS IMPORTANT)
        SUM(qty_in - qty_out)
            OVER (
                PARTITION BY product_id, store_id
                ORDER BY date
            ) AS on_hand_quantity

    FROM combined

)

SELECT * FROM final