WITH
current_from_shapshot as (
   {{ current_from_shapshot(snsh_ref=ref('SNSH_ABC_BANK_POSITION'), output_load_ts=false) }}
)
SELECT
    *
    , POSITION_VALUE - COST_BASE as UNREALIZED_PROFIT
    , ROUND(POSITION_VALUE / COST_BASE, 5) * 100 as UNREALIZED_PROFIT_PCT
FROM current_from_shapshot