-- ============================================================
--  example_query.sql
--  This is a sample SQL file. Replace with your own queries.
--
--  INSTRUCTIONS:
--  1. Add your SQL files to this folder (gab_testing_upload/)
--  2. Push to the main branch on GitHub
--  3. Wait ~60 seconds — a new DAG appears in Airflow
--  4. The DAG runs automatically every day at 12:00 AM
-- ============================================================

SELECT
    current_date     AS run_date,
    current_time     AS run_time,
    'Hello Airflow!' AS message;
