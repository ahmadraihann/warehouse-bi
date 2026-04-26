{{ config(materialized='table') }}

WITH snapshot_data AS (

    SELECT
        *
    FROM {{ ref('stores_snapshot') }}

)

SELECT
    dbt_scd_id AS store_key,
    store_id,
    city,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current
FROM snapshot_data