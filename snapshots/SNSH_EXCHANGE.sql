{% snapshot SNSH_EXCHANGE %}
{{
    config(
        unique_key='EXCHANGE_HKEY',
        strategy='check',
        check_cols=['EXCHANGE_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_EXCHANGE') }}
{% endsnapshot %}