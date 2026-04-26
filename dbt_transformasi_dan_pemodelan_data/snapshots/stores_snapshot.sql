{% snapshot stores_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='store_id',
        strategy='check',
        check_cols=['city']
    )
}}

select * from {{ source('inventory', 'stores') }}

{% endsnapshot %}
