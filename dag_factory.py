"""
dag_factory.py  —  Auto SQL DAG Generator
==========================================
Drop any .sql file into  gab_testing_upload/  in this repo,
push to main, and within 60 seconds a new DAG appears in Airflow.

SETTING A CUSTOM RUN TIME PER SQL FILE:
    Add this comment on the very first line of your .sql file:

        -- schedule: HH:MM

    Examples:
        -- schedule: 00:00   → runs at midnight       (default if not set)
        -- schedule: 09:30   → runs at 9:30 AM daily
        -- schedule: 18:00   → runs at 6:00 PM daily

    Advanced users can also use a full cron expression:
        -- schedule: 0 9 * * 1-5   → 9 AM on weekdays only
        -- schedule: 0 */6 * * *   → every 6 hours
        -- schedule: @weekly       → once a week (Sunday midnight)

FOLDER STRUCTURE IN YOUR REPO:
    Revou_Production_Ready/
    ├── dag_factory.py
    ├── gab_testing_upload/
    │   ├── daily_sales.sql        (-- schedule: 08:00)
    │   ├── midnight_cleanup.sql   (-- schedule: 00:00)
    │   └── hourly_check.sql       (-- schedule: 0 * * * *)
    └── README.md

DATABASE CONNECTION SETUP (one time, in Airflow UI):
    Top menu → Admin → Connections → + (Add)
        Connection Id   :  postgres_default
        Connection Type :  Postgres
        Host            :  your-database-host
        Schema          :  your-database-name
        Login           :  your-username
        Password        :  your-password
        Port            :  5432
"""

import os
import re
import glob
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Folder to watch ─────────────────────────────────────────────────────────
SQL_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gab_testing_upload")

# ── Default schedule if the SQL file has no -- schedule: comment ────────────
DEFAULT_SCHEDULE = "0 0 * * *"   # midnight daily

# ── Shared DAG settings ─────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

# ── Try to load PostgresOperator ────────────────────────────────────────────
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


def _parse_schedule(sql_file_path: str) -> str:
    """
    Reads the first 5 lines of a SQL file looking for:
        -- schedule: HH:MM          → converts to cron  (e.g. 09:30 → "30 9 * * *")
        -- schedule: <cron expr>    → uses as-is        (e.g. "0 9 * * 1-5")
        -- schedule: @keyword       → uses as-is        (e.g. "@weekly")

    Returns DEFAULT_SCHEDULE if no comment found.
    """
    time_pattern = re.compile(r"^--\s*schedule\s*:\s*(.+)", re.IGNORECASE)
    hhmm_pattern = re.compile(r"^(\d{1,2}):(\d{2})$")

    try:
        with open(sql_file_path, "r") as f:
            for _ in range(5):
                line = f.readline().strip()
                match = time_pattern.match(line)
                if match:
                    value = match.group(1).strip()
                    hhmm  = hhmm_pattern.match(value)
                    if hhmm:
                        hour   = int(hhmm.group(1))
                        minute = int(hhmm.group(2))
                        return f"{minute} {hour} * * *"
                    return value   # already a cron expression or @keyword
    except Exception:
        pass

    return DEFAULT_SCHEDULE


def _log_sql(sql_file_path: str, schedule: str, **kwargs):
    """Fallback task — logs the SQL without executing it."""
    with open(sql_file_path, "r") as f:
        content = f.read()
    print("=" * 60)
    print(f"File     : {os.path.basename(sql_file_path)}")
    print(f"Schedule : {schedule}")
    print("=" * 60)
    print(content)
    print("=" * 60)
    print("NOTE: Configure a database connection in Airflow UI")
    print("      Admin → Connections → postgres_default")


# ── Scan folder and generate one DAG per .sql file ──────────────────────────
if not os.path.isdir(SQL_FOLDER):
    import logging
    logging.warning(f"dag_factory: folder not found — {SQL_FOLDER}")
    sql_files = []
else:
    sql_files = glob.glob(os.path.join(SQL_FOLDER, "*.sql"))

for sql_file in sql_files:
    base_name = os.path.splitext(os.path.basename(sql_file))[0]
    safe_name = base_name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
    dag_id    = f"sql__{safe_name}"
    schedule  = _parse_schedule(sql_file)

    with DAG(
        dag_id       = dag_id,
        default_args = DEFAULT_ARGS,
        description  = f"{os.path.basename(sql_file)}  |  schedule: {schedule}",
        schedule     = schedule,
        start_date   = datetime(2024, 1, 1),
        catchup      = False,
        tags         = ["sql", "gab_testing_upload"],
    ) as dag:

        if POSTGRES_AVAILABLE:
            with open(sql_file, "r") as f:
                sql_text = f.read()
            task = PostgresOperator(
                task_id          = "execute_sql",
                postgres_conn_id = "postgres_default",
                sql              = sql_text,
            )
        else:
            task = PythonOperator(
                task_id         = "log_sql",
                python_callable = _log_sql,
                op_kwargs       = {"sql_file_path": sql_file, "schedule": schedule},
            )

    globals()[dag_id] = dag
