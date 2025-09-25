with
src_data as (
    SELECT
        ID as EXCHANGE_CODE -- TEXT
        , NAME as EXCHANGE_NAME -- TEXT
        , COUNTRY as COUNTRY_CODE -- TEXT
        , CITY as CITY_NAME -- TEXT
        , ZONE as TIME_ZONE -- TEXT
        , DELTA as TIME_ZONE_DIFFERENCE -- FLOAT
        , DST_PERIOD as DST_PERIOD -- TEXT
        , OPEN as OPEN_LOCAL_TIME -- TEXT
        , CLOSE as CLOSE_LOCAL_TIME -- TEXT
        , LUNCH as LUNCH_LOCA_TIME -- TEXT
        , OPEN_UTC as OPEN_UTC_TIME -- TEXT
        , CLOSE_UTC as CLOSE_UTC_TIME -- TEXT
        , LUNCH_UTC as LUNCH_UTC_TIME -- TEXT
        , LOAD_TS as LOAD_TS -- TIMESTAMP_NTZ
        , 'SEED.EXCHANGE' as RECORD_SOURCE
    FROM {{ source('seeds', 'EXCHANGE') }}
),
default_record as (
    SELECT
        '-1' as EXCHANGE_CODE
        , 'Missing' as EXCHANGE_NAME 
        , '-1' as COUNTRY_CODE
        , 'Missing' as CITY_NAME
        , 'Missing' as TIME_ZONE
        , 0 as TIME_ZONE_DIFFERENCE
        , 'Missing' as DST_PERIOD
        , 'Missing' as OPEN_LOCAL_TIME
        , 'Missing' as CLOSE_LOCAL_TIME
        , 'Missing' as LUNCH_LOCA_TIME
        , 'Missing' as OPEN_UTC_TIME
        , 'Missing' as CLOSE_UTC_TIME
        , 'Missing' as LUNCH_UTC_TIME
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
        concat_ws('|', EXCHANGE_CODE) as EXCHANGE_HKEY,
        concat_ws('|'
        ,  EXCHANGE_CODE
        ,  EXCHANGE_NAME
        ,  COUNTRY_CODE
        ,  CITY_NAME
        ,  TIME_ZONE
        ,  TIME_ZONE_DIFFERENCE
        ,  DST_PERIOD
        ,  OPEN_LOCAL_TIME
        ,  CLOSE_LOCAL_TIME
        ,  LUNCH_LOCA_TIME
        ,  OPEN_UTC_TIME
        ,  CLOSE_UTC_TIME
        ,  LUNCH_UTC_TIME) as EXCHANGE_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)
SELECT * FROM hashed