{{ config(materialized='table') }}

WITH source AS (

    SELECT
        vendor_id,
        vendor_name
    FROM {{ ref('int_vendors') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER () AS vendor_key,
        vendor_id,
        vendor_name,

        NULL AS vendor_contact,
        NULL AS vendor_address

    FROM source

)

SELECT * FROM final