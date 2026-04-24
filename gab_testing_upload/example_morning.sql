-- schedule: 00:00
-- Runs every day at midnight (12:00 AM)

SELECT
    current_date  AS run_date,
    current_time  AS run_time,
    'midnight job' AS job_name;
