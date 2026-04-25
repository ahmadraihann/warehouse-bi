{{ config(materialized='table') }}

WITH source AS (

    SELECT
        store_id,
        city
    FROM {{ ref('int_stores') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER () AS store_key,

        store_id,
        city

    FROM source

)

SELECT * FROM final