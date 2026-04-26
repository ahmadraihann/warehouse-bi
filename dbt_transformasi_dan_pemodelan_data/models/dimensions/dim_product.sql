{{ config(materialized='table') }}

select
    -- surrogate key (unique per version)
    {{ dbt_utils.generate_surrogate_key([
        'product_id',
        'dbt_valid_from'
    ]) }} as product_key,

    -- natural key
    product_id,

    -- attributes
    description,
    size,
    volume,

    case
        when volume < 500 then 'Small'
        when volume between 500 and 1000 then 'Medium'
        else 'Large'
    end as size_category,

    -- SCD columns
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current

from {{ ref('products_snapshot') }}