with src_data as (
    SELECT
        ALPHABETICCODE as CURRENCY_ALPHABETIC_CODE -- TEXT
        , NUMERICCODE as CURRENCY_NUMERIC_CODE -- NUMBER
        , CURRENCYNAME as CURRENCY_NAME -- TEXT
        , DECIMALDIGITS as DECIMALDIGITS -- NUMBER
        , LOCATIONS as LOCATIONS -- TEXT
        , LOAD_TS as LOAD_TS -- TIMESTAMP_NTZ
        , 'SEED.CURRENCY' as RECORD_SOURCE
    FROM {{ source('seeds', 'CURRENCY') }}
),
default_record as (
    SELECT
        '-1' as CURRENCY_ALPHABETIC_CODE
        , -1 as CURRENCY_NUMERIC_CODE
        , 'Missing' as CURRENCY_NAME
        , -1 as DECIMALDIGITS
        , 'Missing' as LOCATIONS
        , '2000-01-01' as LOAD_TS
        , 'System.DefaultKey' as RECORD_SOURCE
),
with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),
hashed as (
    SELECT
        concat_ws('|', CURRENCY_ALPHABETIC_CODE) as CURRENCY_HKEY
        , concat_ws('|', CURRENCY_ALPHABETIC_CODE, CURRENCY_NUMERIC_CODE, CURRENCY_NAME, DECIMALDIGITS, LOCATIONS) as CURRENCY_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)
SELECT * FROM hashed