# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Orchestration — Manus BioManufacturing Pipeline DAG
# MAGIC
# MAGIC Creates a Databricks Workflow (Jobs API 2.1) with 14 tasks:
# MAGIC
# MAGIC ```
# MAGIC bronze_01 ─┐
# MAGIC bronze_02 ─┤
# MAGIC bronze_03 ─┤─→ bronze_07 → silver_01 → silver_02 → silver_03
# MAGIC bronze_04 ─┤              → gold_01 → gold_02 → gold_03
# MAGIC bronze_05 ─┤              → quality_01
# MAGIC bronze_06 ─┘
# MAGIC ```
# MAGIC
# MAGIC After creation, the workflow URL is printed.
# MAGIC The notebook paths reference the Unity Catalog workspace folder
# MAGIC `/Users/prudrarevanth19@gmail.com/manus-biopipeline/`.

# COMMAND ----------
import json
import requests

# ── Resolve host + token from the running notebook context ──────────────────
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get()           # e.g. https://dbc-3da83634-2907.cloud.databricks.com
token = ctx.apiToken().get()

HEADERS = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

BASE_PATH = "/Users/prudrarevanth19@gmail.com/manus-biopipeline"

def nb(rel_path: str) -> str:
    """Absolute workspace path for a notebook."""
    return f"{BASE_PATH}/{rel_path}"

print(f"Host  : {host}")
print(f"Token : {'*' * 8}{token[-4:]}")
print(f"Notebooks base: {BASE_PATH}")

# ─────────────────────────────────────────────────────────────────────────────
# Workflow definition
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build Workflow JSON

# COMMAND ----------
# Serverless job cluster spec (no cluster id required for serverless)
SERVERLESS = {"job_cluster_key": None}   # tasks will use serverless compute

def task(
    task_key: str,
    notebook_path: str,
    depends_on: list = None,
) -> dict:
    """Build a single Databricks job task definition."""
    t = {
        "task_key":        task_key,
        "notebook_task":   {"notebook_path": notebook_path, "source": "WORKSPACE"},
    }
    if depends_on:
        t["depends_on"] = [{"task_key": k} for k in depends_on]
    return t


workflow = {
    "name": "manus-biopipeline",
    "format": "MULTI_TASK",
    "tasks": [
        # ── Bronze ingest (run in parallel) ───────────────────────────────
        task("bronze_01_batch_manifest",
             nb("bronze/01_batch_manifest")),
        task("bronze_02_bioreactor_sensors",
             nb("bronze/02_bioreactor_sensors")),
        task("bronze_03_lims_results",
             nb("bronze/03_lims_results")),
        task("bronze_04_strain_registry",
             nb("bronze/04_strain_registry")),
        task("bronze_05_work_orders",
             nb("bronze/05_work_orders")),
        task("bronze_06_inventory_snapshots",
             nb("bronze/06_inventory_snapshots")),

        # ── Bronze cleaning (depends on all 6 ingest tasks) ───────────────
        task("bronze_07_data_cleaning",
             nb("bronze/07_data_cleaning"),
             depends_on=[
                 "bronze_01_batch_manifest",
                 "bronze_02_bioreactor_sensors",
                 "bronze_03_lims_results",
                 "bronze_04_strain_registry",
                 "bronze_05_work_orders",
                 "bronze_06_inventory_snapshots",
             ]),

        # ── Silver (sequential) ───────────────────────────────────────────
        task("silver_01_hubs",
             nb("silver/01_hubs"),
             depends_on=["bronze_07_data_cleaning"]),
        task("silver_02_links",
             nb("silver/02_links"),
             depends_on=["silver_01_hubs"]),
        task("silver_03_satellites",
             nb("silver/03_satellites"),
             depends_on=["silver_02_links"]),

        # ── Gold (sequential) ─────────────────────────────────────────────
        task("gold_01_dimensions",
             nb("gold/01_dimensions"),
             depends_on=["silver_03_satellites"]),
        task("gold_02_facts",
             nb("gold/02_facts"),
             depends_on=["gold_01_dimensions"]),
        task("gold_03_kpi_views",
             nb("gold/03_kpi_views"),
             depends_on=["gold_02_facts"]),

        # ── Quality (depends on full gold layer) ──────────────────────────
        task("quality_01_great_expectations",
             nb("quality/01_great_expectations"),
             depends_on=["gold_03_kpi_views"]),
    ],
    "max_concurrent_runs": 1,
    "tags": {"project": "manus-biopipeline", "env": "dev"},
}

print(f"Workflow: {workflow['name']}")
print(f"Tasks   : {len(workflow['tasks'])}")

# ─────────────────────────────────────────────────────────────────────────────
# Create the workflow via REST API
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Workflow

# COMMAND ----------
resp = requests.post(
    f"{host}/api/2.1/jobs/create",
    headers=HEADERS,
    json=workflow,
)

if resp.status_code == 200:
    job_id  = resp.json()["job_id"]
    job_url = f"{host}/#job/{job_id}"
    print(f"[OK] Workflow created")
    print(f"     Job ID  : {job_id}")
    print(f"     Job URL : {job_url}")
else:
    print(f"[ERROR] {resp.status_code}: {resp.text}")
    raise Exception(f"Failed to create workflow: {resp.text}")

# ─────────────────────────────────────────────────────────────────────────────
# Verify — list tasks in created job
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Task Graph

# COMMAND ----------
get_resp = requests.get(
    f"{host}/api/2.1/jobs/get?job_id={job_id}",
    headers=HEADERS,
)

if get_resp.status_code == 200:
    job_def  = get_resp.json()
    tasks    = job_def["settings"]["tasks"]
    print(f"\nJob '{job_def['settings']['name']}' — {len(tasks)} tasks:\n")
    for t in tasks:
        deps = [d["task_key"] for d in t.get("depends_on", [])]
        dep_str = f"← {', '.join(deps)}" if deps else "(no dependencies — parallel start)"
        print(f"  {t['task_key']:<40} {dep_str}")
    print(f"\nWorkflow URL: {job_url}")
else:
    print(f"[ERROR] {get_resp.status_code}: {get_resp.text}")
