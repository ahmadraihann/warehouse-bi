select
    product_id,
    description,
    size,
    volume,
    classification
from {{ source('inventory', 'products') }}
