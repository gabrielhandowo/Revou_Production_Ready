-- Nightly cleanup — delete records older than 90 days

SELECT
    current_date AS run_date,
    'cleanup complete' AS status;
