-- schedule: 09:00
-- Runs every day at 9:00 AM

SELECT
    current_date  AS run_date,
    current_time  AS run_time,
    'morning job' AS job_name;
