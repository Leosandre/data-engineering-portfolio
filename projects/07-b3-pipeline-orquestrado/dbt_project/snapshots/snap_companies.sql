{% snapshot snap_companies %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker',
        strategy='check',
        check_cols=['sector', 'industry']
    )
}}

select
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    current_timestamp as _loaded_at
from {{ source('raw', 'companies') }}

{% endsnapshot %}
