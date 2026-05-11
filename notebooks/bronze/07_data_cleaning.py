# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Data Cleaning & Transformation
# MAGIC
# MAGIC Runs **after** notebooks 01–06 (Bronze ingest) and **before** Silver (Data Vault build).
# MAGIC Reads from `workspace.bronze.*` Delta tables, applies cleaning in-place (overwrite),
# MAGIC and writes a `workspace.bronze.dq_report` audit table plus three clean views.
# MAGIC
# MAGIC | Section | Scope |
# MAGIC |---|---|
# MAGIC | **1 — Missing value handling** | NULL_CRITICAL flags on sensors; fill nulls in LIMS + maintenance |
# MAGIC | **2 — Outlier detection** | Range-based flagging on sensors + LIMS QC metrics |
# MAGIC | **3 — Timestamp alignment** | UTC normalisation, `aligned_timestamp` (ISO 8601), `sensor_hour` |
# MAGIC | **4 — Unit harmonisation maps** | `equipment_mapping` + `material_mapping` reference tables |
# MAGIC | **5 — Business rule validation** | Cross-table integrity: orphan IDs, fail/pass mismatch, invalid dates |
# MAGIC | **6 — DQ summary report** | Audit table: one row per check → `bronze.dq_report` |
# MAGIC | **7 — Clean views** | `*_clean` views (no flagged rows) for Silver to read from |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, DoubleType, LongType, StructType, StructField

CATALOG = "workspace"
SCHEMA  = "bronze"

def tbl(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

def read(name: str) -> DataFrame:
    return spark.table(tbl(name))

def write(df: DataFrame, name: str) -> None:
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(tbl(name)))

# Accumulate DQ stats across all sections
dq_rows        = []   # (table_name, check_type, flag, row_count)
table_row_counts = {} # populated in WRITE step; used by Section 6

def record_dq(table, check, flag, df, condition):
    """Count rows matching condition and record in dq_rows."""
    n = df.filter(condition).count()
    dq_rows.append((table, check, flag, n))
    print(f"    {flag:<35} {n:>10,} rows", flush=True)
    return n

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Missing value handling
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 1 — Missing Value Handling
# MAGIC
# MAGIC * **bioreactor_sensors** — `ph IS NULL` or `reba_titer_g_l IS NULL` → `_dq_flag = 'NULL_CRITICAL'`
# MAGIC * **lims_results** — `analyst` null → `'UNKNOWN'`; `reviewed_by` null → `'PENDING'`
# MAGIC * **work_orders** — `downtime_hours` null where `status = 'Completed'` → `0.0`

# COMMAND ----------
# ── 1a. bioreactor_sensors ────────────────────────────────────────────────
print("1a. bioreactor_sensors — NULL_CRITICAL flagging")

sensors = read("bioreactor_sensors")

sensors_s1 = sensors.withColumn(
    "_dq_flag",
    F.when(
        F.col("ph").isNull() | F.col("reba_titer_g_l").isNull(),
        F.lit("NULL_CRITICAL")
    ).otherwise(F.lit(None).cast(StringType()))
)

record_dq("bioreactor_sensors", "null_check", "NULL_CRITICAL", sensors_s1,
          F.col("_dq_flag") == "NULL_CRITICAL")

# COMMAND ----------
# ── 1b. lims_results ─────────────────────────────────────────────────────
print("\n1b. lims_results — null analyst / reviewed_by fill")

lims = read("lims_results")

null_analyst    = lims.filter(F.col("analyst").isNull()).count()
null_reviewedby = lims.filter(F.col("reviewed_by").isNull()).count()
print(f"    null analyst:     {null_analyst:>10,}")
print(f"    null reviewed_by: {null_reviewedby:>10,}")
dq_rows.append(("lims_results", "null_fill", "NULL_ANALYST",     null_analyst))
dq_rows.append(("lims_results", "null_fill", "NULL_REVIEWED_BY", null_reviewedby))

lims_s1 = (
    lims
    .withColumn("analyst",     F.coalesce(F.col("analyst"),     F.lit("UNKNOWN")))
    .withColumn("reviewed_by", F.coalesce(F.col("reviewed_by"), F.lit("PENDING")))
)

# COMMAND ----------
# ── 1c. work_orders ──────────────────────────────────────────────────────
print("\n1c. work_orders — fill downtime_hours=0.0 for completed WOs with null")

wo = read("work_orders")

null_downtime = wo.filter(
    (F.col("status") == "Completed") & F.col("downtime_hours").isNull()
).count()
print(f"    null downtime (Completed WOs): {null_downtime:>10,}")
dq_rows.append(("work_orders", "null_fill", "NULL_DOWNTIME_COMPLETED", null_downtime))

wo_s1 = wo.withColumn(
    "downtime_hours",
    F.when(
        (F.col("status") == "Completed") & F.col("downtime_hours").isNull(),
        F.lit(0.0)
    ).otherwise(F.col("downtime_hours"))
)

print("\nSection 1 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Outlier detection and flagging
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 2 — Outlier Detection & Flagging
# MAGIC
# MAGIC Flags are written to `_dq_flag` (priority: NULL_CRITICAL > first applicable outlier).
# MAGIC Rows that already carry NULL_CRITICAL keep that flag.
# MAGIC
# MAGIC | Metric | Rule | Flag |
# MAGIC |---|---|---|
# MAGIC | ph | outside 6.0–8.5 | `OUTLIER_PH` |
# MAGIC | temperature_c | outside 30–42 | `OUTLIER_TEMP` |
# MAGIC | dissolved_o2_pct | outside 0–100 | `OUTLIER_DO2` |
# MAGIC | reba_titer_g_l | < 0 | `OUTLIER_TITER` |
# MAGIC | purity_pct | outside 0–100 | `OUTLIER_PURITY` |
# MAGIC | endotoxin_eu_ml | > 5.0 | `OUTLIER_ENDOTOXIN` |

# COMMAND ----------
# ── 2a. bioreactor_sensors ────────────────────────────────────────────────
print("2a. bioreactor_sensors — outlier flagging")

outlier_ph    = F.col("ph").isNotNull()             & ((F.col("ph")              < 6.0)  | (F.col("ph")              > 8.5))
outlier_temp  = F.col("temperature_c").isNotNull()  & ((F.col("temperature_c")  < 30.0) | (F.col("temperature_c")  > 42.0))
outlier_do2   = F.col("dissolved_o2_pct").isNotNull() & ((F.col("dissolved_o2_pct") < 0.0) | (F.col("dissolved_o2_pct") > 100.0))
outlier_titer = F.col("reba_titer_g_l").isNotNull() & (F.col("reba_titer_g_l") < 0.0)

sensors_s2 = sensors_s1.withColumn(
    "_dq_flag",
    F.when(F.col("_dq_flag").isNotNull(), F.col("_dq_flag"))  # preserve NULL_CRITICAL
     .when(outlier_ph,    F.lit("OUTLIER_PH"))
     .when(outlier_temp,  F.lit("OUTLIER_TEMP"))
     .when(outlier_do2,   F.lit("OUTLIER_DO2"))
     .when(outlier_titer, F.lit("OUTLIER_TITER"))
     .otherwise(F.lit(None).cast(StringType()))
)

for flag, cond in [
    ("OUTLIER_PH",    outlier_ph),
    ("OUTLIER_TEMP",  outlier_temp),
    ("OUTLIER_DO2",   outlier_do2),
    ("OUTLIER_TITER", outlier_titer),
]:
    record_dq("bioreactor_sensors", "outlier_check", flag, sensors_s2, cond)

# COMMAND ----------
# ── 2b. lims_results ─────────────────────────────────────────────────────
print("\n2b. lims_results — outlier flagging")

outlier_purity    = F.col("purity_pct").isNotNull()     & ((F.col("purity_pct")    < 0.0) | (F.col("purity_pct")    > 100.0))
outlier_endotoxin = F.col("endotoxin_eu_ml").isNotNull() & (F.col("endotoxin_eu_ml") > 5.0)

lims_s2 = lims_s1.withColumn(
    "_dq_flag",
    F.when(outlier_purity,    F.lit("OUTLIER_PURITY"))
     .when(outlier_endotoxin, F.lit("OUTLIER_ENDOTOXIN"))
     .otherwise(F.lit(None).cast(StringType()))
)

for flag, cond in [
    ("OUTLIER_PURITY",    outlier_purity),
    ("OUTLIER_ENDOTOXIN", outlier_endotoxin),
]:
    record_dq("lims_results", "outlier_check", flag, lims_s2, cond)

print("\nSection 2 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — Timestamp alignment
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 3 — Timestamp Alignment
# MAGIC
# MAGIC For every Bronze table:
# MAGIC * All existing timestamp columns → forced UTC via `to_utc_timestamp()`
# MAGIC * `aligned_timestamp` → ISO 8601 string of the table's primary event timestamp
# MAGIC * `sensor_hour` (bioreactor_sensors only) → `timestamp` truncated to hour for LIMS joins

# COMMAND ----------
# ── 3a. bioreactor_sensors ────────────────────────────────────────────────
print("3a. bioreactor_sensors — UTC + aligned_timestamp + sensor_hour")

sensors_s3 = (
    sensors_s2
    .withColumn("timestamp",         F.to_utc_timestamp("timestamp", "UTC"))
    .withColumn("aligned_timestamp", F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("sensor_hour",       F.date_trunc("hour", F.col("timestamp")))
)

# COMMAND ----------
# ── 3b. batch_manifest ────────────────────────────────────────────────────
print("3b. batch_manifest — UTC + aligned_timestamp")

batch = read("batch_manifest")
batch_s3 = (
    batch
    .withColumn("start_time",        F.to_utc_timestamp("start_time", "UTC"))
    .withColumn("end_time",          F.to_utc_timestamp("end_time",   "UTC"))
    .withColumn("aligned_timestamp", F.date_format("start_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# COMMAND ----------
# ── 3c. lims_results ─────────────────────────────────────────────────────
print("3c. lims_results — UTC + aligned_timestamp")

lims_s3 = (
    lims_s2
    .withColumn("sampled_at",        F.to_utc_timestamp("sampled_at",  "UTC"))
    .withColumn("reviewed_at",       F.to_utc_timestamp("reviewed_at", "UTC"))
    .withColumn("aligned_timestamp", F.date_format("sampled_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# COMMAND ----------
# ── 3d. inventory_snapshots ───────────────────────────────────────────────
print("3d. inventory_snapshots — UTC + aligned_timestamp")

inv = read("inventory_snapshots")
inv_s3 = (
    inv
    .withColumn("snapshot_ts",       F.to_utc_timestamp("snapshot_ts", "UTC"))
    .withColumn("aligned_timestamp", F.date_format("snapshot_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# COMMAND ----------
# ── 3e. work_orders ───────────────────────────────────────────────────────
print("3e. work_orders — UTC + aligned_timestamp")

wo_s3 = (
    wo_s1
    .withColumn("created_at",        F.to_utc_timestamp("created_at",     "UTC"))
    .withColumn("scheduled_date",    F.to_utc_timestamp("scheduled_date",  "UTC"))
    .withColumn("completed_at",      F.to_utc_timestamp("completed_at",    "UTC"))
    .withColumn("aligned_timestamp", F.date_format("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

# COMMAND ----------
# ── 3f. strain_registry ───────────────────────────────────────────────────
print("3f. strain_registry — UTC + aligned_timestamp")

strain = read("strain_registry")
strain_s3 = (
    strain
    .withColumn("created_at",        F.to_utc_timestamp("created_at", "UTC"))
    .withColumn("aligned_timestamp", F.date_format("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

print("\nSection 3 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Unit harmonisation mapping tables
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 4 — Unit Harmonisation Mapping Tables
# MAGIC
# MAGIC Two reference tables written as Delta to `workspace.bronze.*`:
# MAGIC
# MAGIC * **`equipment_mapping`** — canonical equipment metadata (full name, scale level, area, vessel volume)
# MAGIC * **`material_mapping`** — raw material reference with standard unit and category
# MAGIC
# MAGIC These are the single source of truth for unit conversions and equipment joins in Silver/Gold.

# COMMAND ----------
# ── 4a. equipment_mapping ─────────────────────────────────────────────────
print("4a. equipment_mapping — creating reference table (12 rows)")

equip_schema = StructType([
    StructField("equipment_id", StringType(), False),
    StructField("full_name",    StringType(), True),
    StructField("scale_level",  StringType(), True),
    StructField("area",         StringType(), True),
    StructField("volume_l",     DoubleType(), True),
])

equipment_data = [
    ("BR-LAB-01",    "Bioreactor Lab 1",      "lab",           "Fermentation", 2.0),
    ("BR-LAB-02",    "Bioreactor Lab 2",      "lab",           "Fermentation", 2.0),
    ("BR-LAB-03",    "Bioreactor Lab 3",      "lab",           "Fermentation", 2.0),
    ("BR-LAB-04",    "Bioreactor Lab 4",      "lab",           "Fermentation", 2.0),
    ("BR-PILOT-01",  "Bioreactor Pilot 1",    "pilot",         "Fermentation", 500.0),
    ("BR-PILOT-02",  "Bioreactor Pilot 2",    "pilot",         "Fermentation", 500.0),
    ("BR-MFG-01",    "Bioreactor Mfg 1",      "manufacturing", "Fermentation", 50000.0),
    ("CF-01",        "Centrifuge",            "pilot",         "Downstream",   None),
    ("UF-01",        "Ultrafiltration Unit",  "pilot",         "Downstream",   None),
    ("CIP-01",       "CIP Skid",             "manufacturing", "Utilities",    None),
    ("HVAC-01",      "HVAC Unit",            "manufacturing", "Utilities",    None),
    ("AUTOCLAVE-01", "Autoclave",            "lab",           "QC Lab",       None),
]

equip_df = spark.createDataFrame(equipment_data, schema=equip_schema)
write(equip_df, "equipment_mapping")
print(f"  [OK] equipment_mapping — {equip_df.count()} rows")

# COMMAND ----------
# ── 4b. material_mapping ──────────────────────────────────────────────────
print("\n4b. material_mapping — creating reference table (7 rows)")

mat_schema = StructType([
    StructField("material_id",   StringType(), False),
    StructField("material_name", StringType(), True),
    StructField("unit_standard", StringType(), True),
    StructField("category",      StringType(), True),
])

material_data = [
    ("RM-001", "Glucose",                  "kg",   "Carbon Source"),
    ("RM-002", "Yeast Extract",            "kg",   "Nitrogen Source"),
    ("RM-003", "Ammonium Sulfate",         "kg",   "Nitrogen Source"),
    ("RM-004", "Antifoam Agent",           "L",    "Process Aid"),
    ("RM-005", "NaOH 10%",                "L",    "pH Control"),
    ("RM-006", "HCl 10%",                 "L",    "pH Control"),
    ("RM-007", "DO Calibration Standard", "unit", "Calibration"),
]

mat_df = spark.createDataFrame(material_data, schema=mat_schema)
write(mat_df, "material_mapping")
print(f"  [OK] material_mapping — {mat_df.count()} rows")

print("\nSection 4 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Business rule validation
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 5 — Business Rule Validation
# MAGIC
# MAGIC Cross-table integrity checks. Business rule flags override outlier flags.
# MAGIC
# MAGIC | Rule | Table | Condition | Flag |
# MAGIC |---|---|---|---|
# MAGIC | 1 | lims_results | batch_id not in batch_manifest | `ORPHAN_BATCH_ID` |
# MAGIC | 2 | lims_results | batch status FAILED/ABORTED AND lims status PASS | `RULE_FAIL_PASS_MISMATCH` |
# MAGIC | 3 | bioreactor_sensors | batch_id not in batch_manifest | `UNKNOWN_BATCH` |
# MAGIC | 4 | work_orders | completed_at < created_at | `RULE_INVALID_DATES` |

# COMMAND ----------
# ── 5a. Reference sets from batch_manifest ────────────────────────────────
print("5a. Building reference sets from batch_manifest")

valid_batches = (
    batch_s3
    .select(F.col("batch_id").alias("_ref_bid"))
    .distinct()
)

failed_batches = (
    batch_s3
    .filter(F.col("status").isin("FAILED", "ABORTED"))
    .select(F.col("batch_id").alias("_failed_bid"))
    .distinct()
)

print(f"    valid batch IDs:          {valid_batches.count():>8,}")
print(f"    failed/aborted batch IDs: {failed_batches.count():>8,}")

# COMMAND ----------
# ── 5b. lims_results — Rule 1 + Rule 2 ───────────────────────────────────
print("\n5b. lims_results — Rule 1 (ORPHAN_BATCH_ID) + Rule 2 (RULE_FAIL_PASS_MISMATCH)")

lims_s5 = (
    lims_s3
    .join(valid_batches,  lims_s3["batch_id"] == F.col("_ref_bid"),    how="left")
    .join(failed_batches, lims_s3["batch_id"] == F.col("_failed_bid"), how="left")
    .withColumn(
        "_dq_flag",
        F.when(F.col("_ref_bid").isNull(),
               F.lit("ORPHAN_BATCH_ID"))
         .when(F.col("_failed_bid").isNotNull() & (F.col("status") == "PASS"),
               F.lit("RULE_FAIL_PASS_MISMATCH"))
         .otherwise(F.col("_dq_flag"))
    )
    .drop("_ref_bid", "_failed_bid")
)

for flag, cond in [
    ("ORPHAN_BATCH_ID",         F.col("_dq_flag") == "ORPHAN_BATCH_ID"),
    ("RULE_FAIL_PASS_MISMATCH", F.col("_dq_flag") == "RULE_FAIL_PASS_MISMATCH"),
]:
    record_dq("lims_results", "rule_check", flag, lims_s5, cond)

# COMMAND ----------
# ── 5c. bioreactor_sensors — Rule 3 ──────────────────────────────────────
print("\n5c. bioreactor_sensors — Rule 3 (UNKNOWN_BATCH)")

sensors_s5 = (
    sensors_s3
    .join(valid_batches, sensors_s3["batch_id"] == F.col("_ref_bid"), how="left")
    .withColumn(
        "_dq_flag",
        F.when(F.col("_ref_bid").isNull(), F.lit("UNKNOWN_BATCH"))
         .otherwise(F.col("_dq_flag"))
    )
    .drop("_ref_bid")
)

record_dq("bioreactor_sensors", "rule_check", "UNKNOWN_BATCH", sensors_s5,
          F.col("_dq_flag") == "UNKNOWN_BATCH")

# COMMAND ----------
# ── 5d. work_orders — Rule 4 ──────────────────────────────────────────────
print("\n5d. work_orders — Rule 4 (RULE_INVALID_DATES)")

wo_s5 = wo_s3.withColumn(
    "_dq_flag",
    F.when(
        F.col("completed_at").isNotNull() &
        F.col("created_at").isNotNull() &
        (F.col("completed_at") < F.col("created_at")),
        F.lit("RULE_INVALID_DATES")
    ).otherwise(F.lit(None).cast(StringType()))
)

record_dq("work_orders", "rule_check", "RULE_INVALID_DATES", wo_s5,
          F.col("_dq_flag") == "RULE_INVALID_DATES")

print("\nSection 5 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# WRITE — overwrite Bronze tables with cleaned + enriched data
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Write — Overwrite Bronze Tables with Cleaned Data

# COMMAND ----------
print("Writing cleaned Bronze tables...")

for df, name in [
    (sensors_s5,   "bioreactor_sensors"),
    (lims_s5,      "lims_results"),
    (wo_s5,        "work_orders"),
    (batch_s3,     "batch_manifest"),
    (inv_s3,       "inventory_snapshots"),
    (strain_s3,    "strain_registry"),
]:
    write(df, name)
    n = spark.table(tbl(name)).count()
    table_row_counts[name] = n
    print(f"  [OK] {name:<30} {n:>12,} rows", flush=True)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — DQ summary report (audit trail)
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 6 — DQ Summary Report
# MAGIC
# MAGIC One row per check across all Bronze tables.
# MAGIC Columns: `table_name | check_name | records_checked | records_flagged | flagged_pct | run_timestamp`
# MAGIC
# MAGIC Written to `workspace.bronze.dq_report` — the audit trail for every run of this notebook.

# COMMAND ----------
print("Section 6 — building DQ audit trail")

# Exclude "summary" pseudo-rows; keep only real check entries
check_rows = [
    (
        table,
        f"{check_type}:{flag}",
        int(table_row_counts.get(table, 0)),
        int(cnt),
    )
    for table, check_type, flag, cnt in dq_rows
    if check_type != "summary"
]

s6_schema = StructType([
    StructField("table_name",      StringType(), True),
    StructField("check_name",      StringType(), True),
    StructField("records_checked", LongType(),   True),
    StructField("records_flagged", LongType(),   True),
])

dq_audit = (
    spark.createDataFrame(check_rows, schema=s6_schema)
    .withColumn(
        "flagged_pct",
        F.when(F.col("records_checked") > 0,
               F.round(F.col("records_flagged") / F.col("records_checked") * 100, 6))
        .otherwise(F.lit(0.0))
    )
    .withColumn("run_timestamp", F.current_timestamp())
    .orderBy("table_name", "check_name")
)

write(dq_audit, "dq_report")
n_checks = dq_audit.count()
print(f"  [OK] dq_report — {n_checks} check rows written")
print("\nSection 6 complete.")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — Write cleaned views
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Section 7 — Clean Views
# MAGIC
# MAGIC For each Bronze table that carries `_dq_flag`, a view is created that exposes only
# MAGIC rows where `_dq_flag IS NULL` (i.e. no quality issue detected).
# MAGIC Silver reads exclusively from these views — never from the raw Bronze table.
# MAGIC
# MAGIC | View | Source table | Filters |
# MAGIC |---|---|---|
# MAGIC | `bronze.bioreactor_sensors_clean` | `bronze.bioreactor_sensors` | `_dq_flag IS NULL` |
# MAGIC | `bronze.lims_results_clean` | `bronze.lims_results` | `_dq_flag IS NULL` |
# MAGIC | `bronze.work_orders_clean` | `bronze.work_orders` | `_dq_flag IS NULL` |

# COMMAND ----------
print("Section 7 — creating clean views")

for view_name, source_table in [
    ("bioreactor_sensors_clean", "bioreactor_sensors"),
    ("lims_results_clean",       "lims_results"),
    ("work_orders_clean",        "work_orders"),
]:
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.{view_name}
        AS
        SELECT * FROM {CATALOG}.{SCHEMA}.{source_table}
        WHERE _dq_flag IS NULL
    """)
    n_clean = spark.table(f"{CATALOG}.{SCHEMA}.{view_name}").count()
    n_total = table_row_counts.get(source_table, 0)
    pct     = round(n_clean / n_total * 100, 2) if n_total else 0.0
    print(f"  [OK] {view_name:<35} {n_clean:>12,} / {n_total:>12,} rows  ({pct}% clean)")

print("\nSection 7 complete.")
print("\n── Bronze cleaning pipeline finished ─────────────────────────────────")

# COMMAND ----------
# MAGIC %md ## DQ Report — Audit Trail

# COMMAND ----------
display(
    spark.table(tbl("dq_report"))
    .orderBy("table_name", "check_name")
)
