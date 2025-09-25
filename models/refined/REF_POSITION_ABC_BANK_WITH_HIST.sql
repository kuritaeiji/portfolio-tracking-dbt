WITH current_from_history as (
    {{ current_from_history(
        history_rel = ref('HIST_ABC_BANK_POSITION_WITH_CLOSING'),
        key_column = 'POSITION_HKEY'
    ) }}
),
security_history as (
    SELECT
        *
        , LOAD_TS_UTC as START_TS_UTC
        , COALESCE(
            DATEADD(nanosecond, -1,  LEAD(LOAD_TS_UTC) OVER (PARTITION BY SECURITY_HKEY ORDER BY LOAD_TS_UTC)),
            '9999-12-31 23:59:59.999999999'::TIMESTAMP_NTZ
        ) as ENT_TS_UTC
    FROM {{ ref('HIST_ABC_BANK_SECURITY_INFO') }}
)
SELECT
    p.*
    , s.SECURITY_HDIFF
    , p.POSITION_VALUE - p.COST_BASE as UNREALIZED_PROFIT
    , ROUND(DIV0(p.POSITION_VALUE, p.COST_BASE), 5) * 100 as UNREALIZED_PROFIT_PCT
FROM current_from_history as p
INNER JOIN security_history as s
ON p.SECURITY_CODE = s.SECURITY_CODE
WHERE p.LOAD_TS_UTC BETWEEN s.START_TS_UTC AND s.ENT_TS_UTC