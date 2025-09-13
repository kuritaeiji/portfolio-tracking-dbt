with test_data as (
    SELECT '0021-09-23'::date as src_date,
           '2021-09-23'::date as expected_date
    UNION ALL
    SELECT '1021-09-24'::date, '1021-09-24'::date
    UNION ALL
    SELECT '2021-09-25'::date, '2021-09-25'::date
    UNION ALL
    SELECT '-0021-09-26'::date, '1979-09-26'::date
)
SELECT
    {{ to_21st_century_date(date_column_name='src_date') }} as actual_date,
    expected_date,
    actual_date = expected_date as matching
FROM test_data
WHERE not matching