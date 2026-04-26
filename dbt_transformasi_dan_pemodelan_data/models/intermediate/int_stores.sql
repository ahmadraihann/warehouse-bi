{{ config(materialized='view') }}

select
    store_id,
    city
from {{ ref('stg_stores') }}
