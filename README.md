# Revou Production Ready — Airflow Setup Guide

This guide explains how to run Airflow from scratch on your computer.

---

## What You Need Before Starting

- A computer running **Windows 10 or newer**
- The **airflow_setup.zip** file (shared with you separately)
- A stable internet connection (first run downloads ~1 GB of files)

---

## Step 1 — Install Docker Desktop

Docker is the engine that runs Airflow. You only need to do this once.

1. Open your browser and go to:
   **https://www.docker.com/products/docker-desktop**

2. Click **"Download Docker Desktop"** → choose **Windows**

3. Open the downloaded file and click **Yes** when the permission popup appears

4. Wait for the installation to finish (2–5 minutes)

5. **Restart your computer** when it asks — this step is required

6. After restarting, find **Docker Desktop** in your Start menu and open it

7. Wait until the bottom of the Docker Desktop window shows:

   > ✅ Engine running

   This can take 1–2 minutes on first launch. Do not skip this step.

---

## Step 2 — Unzip the Project

1. Find the **airflow_setup.zip** file you downloaded
2. Right-click it → **Extract All**
3. Choose a folder you can remember (e.g. `C:\airflow` or your Desktop)
4. After extracting you will see:

```
📁 your chosen folder
├── START_HERE.bat        ← Windows setup file
├── START_HERE.sh         ← Mac / Linux setup file
└── airflow-project/      ← all project files live here
```

---

## Step 3 — Run START_HERE.bat (Windows)/ START_HERE.sh (mac/Linux)

1. Make sure **Docker Desktop is open** and shows **"Engine running"**

2. Go to the folder where you extracted the zip

3. **Double-click** `START_HERE.bat`

4. A black Command Prompt window will open — **do not close it**

5. The script will:
   - ✅ Check Docker is installed
   - ✅ Set up the Airflow database (first run only — takes ~2 minutes)
   - ✅ Start all services
   - ✅ Wait until Airflow is ready
   - ✅ Open your browser automatically

> **First run takes about 5–10 minutes** because Docker needs to download
> all the Airflow images. Subsequent runs take less than 1 minute.

---

## Step 4 — Open Airflow in Your Browser

Once the script finishes, your browser will open automatically.

If it does not open on its own, go to:

> **http://localhost:8080**

Log in with:

| Field    | Value   |
|----------|---------|
| Username | `admin` |
| Password | `admin` |

You should see the Airflow dashboard with your DAGs listed.

---

## Daily Use — Starting and Stopping Airflow

### To START Airflow

1. Open **Docker Desktop** and wait for **"Engine running"**
2. Double-click **START_HERE.bat**
3. Go to **http://localhost:8080**

### To STOP Airflow

Open a Command Prompt inside the `airflow-project` folder and run:

```
docker compose down
```

Or use the **Makefile** shortcut (if you have `make` installed):

```
make stop
```

### To CHECK if Airflow is running

Open your browser and go to **http://localhost:8080**.
If you see the login page, Airflow is running.
If the page does not load, Airflow is stopped — run START_HERE.bat to start it.

---

## Adding New Automated SQL Jobs

1. Create a new folder inside this repo (e.g. `my_new_job/`)
2. Add a `dag_config.yaml` file inside it:

```yaml
job_name    : my_new_job
label       : "My New Job"
schedule    : "08:00"
description : "Runs every day at 8 AM"
owner       : your_name
```

3. Add your `.sql` files to the same folder
4. Push everything to the **main** branch on GitHub
5. Wait **60 seconds** — the new DAG appears in Airflow automatically

> SQL files run in alphabetical order.
> Prefix them with `01_`, `02_`, `03_` to control the sequence.

---

## Troubleshooting

**"docker is not recognized" error**
Docker was just installed but Windows hasn't updated yet.
→ Restart your computer and run START_HERE.bat again.

**Browser shows a blank page**
Airflow is still starting up.
→ Wait 1–2 minutes and press **Ctrl + Shift + R** to hard-refresh.

**Docker Desktop won't start**
Your computer may not have enough free memory.
→ Close other apps and try again. Docker needs at least 4 GB of RAM.

**DAGs not showing up after a push**
→ Wait 60 seconds and refresh the Airflow page.
→ Check the git-sync log: open a Command Prompt in `airflow-project/` and run:
   `docker compose logs git-sync --tail=20`

**Port 8080 already in use**
Another app is using port 8080.
→ Change the port in `airflow-project/docker-compose.yml`:
```yaml
ports:
  - "8888:8080"
```
Then use **http://localhost:8888** instead.

**Full reset — start everything from scratch**
Open a Command Prompt in `airflow-project/` and run:
```
docker compose down --volumes
```
Then run START_HERE.bat again.
⚠️ This deletes all Airflow data. Your SQL files in GitHub are not affected.

---

## Quick Reference

| Action | How |
|--------|-----|
| Start Airflow | Double-click `START_HERE.bat` |
| Open Airflow UI | http://localhost:8080 |
| Stop Airflow | `docker compose down` in airflow-project/ |
| View logs | `docker compose logs -f` in airflow-project/ |
| Add a new job | Create folder + `dag_config.yaml` + `.sql` → push to GitHub |
| Force refresh DAGs | Wait 60 sec after pushing to GitHub |

