select
    product_id,
    description,
    size,
    volume,
    classification
from {{ ref('stg_products') }}
