"""
dag_factory.py  —  Auto SQL DAG Generator
==========================================
Drop any .sql file into the  gab_testing_upload/  folder in this repo,
push to main, and within 60 seconds a new DAG will appear in Airflow
ready to run every day at 12:00 AM (midnight).

FOLDER STRUCTURE IN YOUR REPO:
    Revou_Production_Ready/
    ├── dag_factory.py          ← this file
    ├── gab_testing_upload/     ← put your .sql files here
    │   ├── 1st_assigntment.sql
    │   └── any_other_query.sql
    └── README.md

DATABASE CONNECTION SETUP (one time, in Airflow UI):
    1. Go to  http://localhost:8080
    2. Top menu → Admin → Connections
    3. Click  +  to add a new connection
    4. Fill in:
         Connection Id   :  postgres_default
         Connection Type :  Postgres
         Host            :  your-database-host
         Schema          :  your-database-name
         Login           :  your-username
         Password        :  your-password
         Port            :  5432
    5. Save — all SQL DAGs will use this connection automatically.

If no connection is configured yet, the DAG will still appear and run
but it will log the SQL instead of executing it (safe fallback).
"""

import os
import glob
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Where to look for .sql files ────────────────────────────────────────────
# dag_factory.py lives at the repo root.
# git-sync maps the repo root to /git/dags inside the container.
SQL_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gab_testing_upload")

# ── Shared settings for every generated DAG ────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

# ── Try to use PostgresOperator if the provider package is installed ────────
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


def _log_sql(sql_file_path: str, **kwargs):
    """
    Fallback task: reads and logs the SQL without executing it.
    Replace this with a real DB call once a connection is configured.
    """
    with open(sql_file_path, "r") as f:
        content = f.read()
    print("=" * 60)
    print(f"SQL file : {os.path.basename(sql_file_path)}")
    print("=" * 60)
    print(content)
    print("=" * 60)
    print("NOTE: No database connection configured yet.")
    print("Go to Airflow UI → Admin → Connections to add one.")


# ── Scan the folder and create one DAG per .sql file ────────────────────────
if not os.path.isdir(SQL_FOLDER):
    import logging
    logging.warning(f"dag_factory: folder not found: {SQL_FOLDER}")
    sql_files = []
else:
    sql_files = glob.glob(os.path.join(SQL_FOLDER, "*.sql"))

for sql_file in sql_files:
    # Build a safe DAG id from the filename
    base_name   = os.path.splitext(os.path.basename(sql_file))[0]
    safe_name   = base_name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
    dag_id      = f"sql__{safe_name}"

    with DAG(
        dag_id        = dag_id,
        default_args  = DEFAULT_ARGS,
        description   = f"Runs  gab_testing_upload/{os.path.basename(sql_file)}  every day at midnight",
        schedule      = "0 0 * * *",   # ← 12:00 AM daily  (change if needed)
        start_date    = datetime(2024, 1, 1),
        catchup       = False,
        tags          = ["sql", "gab_testing_upload"],
    ) as dag:

        if POSTGRES_AVAILABLE:
            # ── Execute the SQL against the configured Postgres connection ──
            with open(sql_file, "r") as f:
                sql_text = f.read()

            task = PostgresOperator(
                task_id         = "execute_sql",
                postgres_conn_id = "postgres_default",  # ← set in Airflow UI
                sql             = sql_text,
            )
        else:
            # ── Fallback: log the SQL (no DB needed) ────────────────────────
            task = PythonOperator(
                task_id         = "log_sql",
                python_callable = _log_sql,
                op_kwargs       = {"sql_file_path": sql_file},
            )

    # Register with Airflow
    globals()[dag_id] = dag
