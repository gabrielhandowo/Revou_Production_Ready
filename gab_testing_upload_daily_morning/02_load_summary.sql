-- Step 1: Extract daily sales
-- This runs first (prefix 01_ controls order)

SELECT
    current_date AS report_date,
    'extracting sales data...' AS status;
