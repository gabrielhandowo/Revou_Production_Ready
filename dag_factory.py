"""
dag_factory.py  —  Auto SQL DAG Generator
==========================================
How it works:
  1. Scans every subfolder in this repo for a  dag_config.yaml  file
  2. Each folder with a config file becomes ONE DAG in Airflow
  3. Every .sql file inside that folder becomes ONE TASK in that DAG
  4. Tasks run in alphabetical order (prefix filenames with 01_, 02_ to control order)

Push any folder + dag_config.yaml + .sql files to GitHub → DAG appears in 60 seconds.

FOLDER STRUCTURE EXAMPLE:
    Revou_Production_Ready/
    ├── dag_factory.py
    ├── gab_testing_upload_daily_morning/
    │   ├── dag_config.yaml          ← schedule, label, job name
    │   ├── 01_extract_sales.sql
    │   └── 02_load_summary.sql
    ├── gab_testing_upload_nightly/
    │   ├── dag_config.yaml
    │   └── cleanup_old_records.sql
    └── README.md

DATABASE CONNECTION (one time setup in Airflow UI):
    Admin → Connections → + (Add)
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
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config filename to look for inside each folder ──────────────────────────
CONFIG_FILENAME = "dag_config.yaml"

# ── Root of the repo (where dag_factory.py lives) ───────────────────────────
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Shared DAG defaults ─────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner"           : "airflow",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=10),
    "email_on_failure": False,
}

# ── Optional: use PostgresOperator if the provider is installed ─────────────
try:
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

# ── Optional: PyYAML (always available inside Airflow containers) ────────────
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logging.warning("dag_factory: PyYAML not found — dag_config.yaml files cannot be read.")


# ════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _parse_schedule(value: str) -> str:
    """
    Accepts:
      HH:MM          → converted to cron  (e.g. "09:30"  →  "30 9 * * *")
      cron string    → used as-is         (e.g. "0 8 * * 1-5")
      @keyword       → used as-is         (e.g. "@daily", "@weekly")
    """
    value = str(value).strip()
    hhmm  = re.match(r"^(\d{1,2}):(\d{2})$", value)
    if hhmm:
        h = int(hhmm.group(1))
        m = int(hhmm.group(2))
        return f"{m} {h} * * *"
    return value


def _make_task_id(sql_filename: str) -> str:
    """Turn a filename into a valid Airflow task id."""
    name = os.path.splitext(sql_filename)[0]
    return re.sub(r"[^a-zA-Z0-9_\-\.]", "_", name).lower()


def _log_sql_task(sql_file_path: str, job_name: str, **kwargs):
    """
    Fallback task used when no Postgres connection is configured.
    Logs the SQL to the Airflow task log so you can review it.
    """
    with open(sql_file_path, "r") as f:
        content = f.read()
    print("=" * 60)
    print(f"Job      : {job_name}")
    print(f"SQL file : {os.path.basename(sql_file_path)}")
    print("=" * 60)
    print(content)
    print("=" * 60)
    print("NOTE: Set up a Postgres connection in Airflow UI to execute this SQL.")
    print("      Admin → Connections → postgres_default")


# ════════════════════════════════════════════════════════════════════════════
#  MAIN: scan subfolders and generate DAGs
# ════════════════════════════════════════════════════════════════════════════

if YAML_AVAILABLE:
    for entry in sorted(os.listdir(ROOT_DIR)):
        folder_path = os.path.join(ROOT_DIR, entry)

        # Only process directories
        if not os.path.isdir(folder_path):
            continue

        # Only process folders that contain a dag_config.yaml
        config_path = os.path.join(folder_path, CONFIG_FILENAME)
        if not os.path.exists(config_path):
            continue

        # ── Read dag_config.yaml ─────────────────────────────────────────
        try:
            with open(config_path, "r") as f:
                cfg = yaml.safe_load(f) or {}
        except Exception as e:
            logging.error(f"dag_factory: could not read {config_path}: {e}")
            continue

        # ── Extract config values ────────────────────────────────────────
        job_name    = cfg.get("job_name",    entry)           # DAG id in Airflow
        label       = cfg.get("label",       job_name)        # Human-readable name
        schedule    = _parse_schedule(cfg.get("schedule", "00:00"))
        description = cfg.get("description", f"Auto-generated from {entry}/")
        owner       = cfg.get("owner",       "airflow")

        # ── Find all SQL files in this folder (sorted = run order) ───────
        sql_files = sorted(glob.glob(os.path.join(folder_path, "*.sql")))
        if not sql_files:
            logging.warning(f"dag_factory: no .sql files found in {folder_path} — skipping.")
            continue

        # ── Build the DAG ────────────────────────────────────────────────
        dag_args = {**DEFAULT_ARGS, "owner": owner}

        with DAG(
            dag_id       = job_name,
            default_args = dag_args,
            description  = f"{label}  |  {description}",
            schedule     = schedule,
            start_date   = datetime(2024, 1, 1),
            catchup      = False,
            tags         = ["sql", entry],
        ) as dag:

            previous_task = None

            for sql_file in sql_files:
                task_id = _make_task_id(os.path.basename(sql_file))

                if POSTGRES_AVAILABLE:
                    with open(sql_file, "r") as f:
                        sql_text = f.read()
                    current_task = PostgresOperator(
                        task_id          = task_id,
                        postgres_conn_id = "postgres_default",
                        sql              = sql_text,
                    )
                else:
                    current_task = PythonOperator(
                        task_id         = task_id,
                        python_callable = _log_sql_task,
                        op_kwargs       = {
                            "sql_file_path": sql_file,
                            "job_name"     : job_name,
                        },
                    )

                # Chain tasks sequentially: task1 >> task2 >> task3 ...
                if previous_task:
                    previous_task >> current_task
                previous_task = current_task

        # Register DAG with Airflow
        globals()[job_name] = dag
        logging.info(f"dag_factory: registered DAG '{job_name}' ({len(sql_files)} SQL files, schedule={schedule})")
