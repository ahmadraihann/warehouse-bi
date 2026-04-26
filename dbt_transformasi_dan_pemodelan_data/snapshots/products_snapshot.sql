{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['description', 'size', 'volume']
    )
}}

select * from {{ source('inventory', 'products') }}

{% endsnapshot %}