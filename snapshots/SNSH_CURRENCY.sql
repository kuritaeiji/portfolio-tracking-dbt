{% snapshot SNSH_CURRENCY %}
{{
    config(
        unique_key='CURRENCY_HKEY',
        strategy='check',
        check_cols=['CURRENCY_HDIFF']
    )
}}
SELECT * FROM {{ ref('STG_CURRENCY') }}
{% endsnapshot %}