SELECT
    date_trunc(current_date(),month) AS report_date,
    'extracting sales data...' AS status