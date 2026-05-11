# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold — Star Schema: Dimensions
# MAGIC
# MAGIC Reads from `workspace.silver.*` and `workspace.bronze.*` reference tables.
# MAGIC Writes to `workspace.gold.*`.
# MAGIC
# MAGIC | Dimension | Grain | Source |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | One row per calendar day | Generated from batch date range |
# MAGIC | `dim_batch` | One row per batch run | `bronze.batch_manifest` |
# MAGIC | `dim_strain` | One row per engineered strain | `silver.sat_strain_engineering` |
# MAGIC | `dim_equipment` | One row per equipment unit | `bronze.equipment_mapping` |
# MAGIC | `dim_material` | One row per raw material | `bronze.material_mapping` |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

CATALOG = "workspace"
GOLD    = "gold"
SILVER  = "silver"
BRONZE  = "bronze"

def gtbl(n): return f"{CATALOG}.{GOLD}.{n}"
def stbl(n): return f"{CATALOG}.{SILVER}.{n}"
def btbl(n): return f"{CATALOG}.{BRONZE}.{n}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")
print(f"Schema {CATALOG}.{GOLD} ready.")

def write_dim(df: DataFrame, name: str) -> None:
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(gtbl(name)))
    n = spark.table(gtbl(name)).count()
    print(f"  [OK] {name:<28} {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# dim_date
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_date
# MAGIC Calendar spine covering the full batch timeline.
# MAGIC `date_key` (YYYYMMDD integer) is the join key used in fact tables.

# COMMAND ----------
print("dim_date — generating calendar spine from batch date range")

bounds = (
    spark.table(btbl("batch_manifest"))
    .agg(
        F.to_date(F.min("start_time")).alias("min_date"),
        F.to_date(F.max("end_time")).alias("max_date"),
    )
    .collect()[0]
)
min_d, max_d = bounds.min_date, bounds.max_date
print(f"    Date range: {min_d} → {max_d}")

dim_date = (
    spark.sql(f"SELECT explode(sequence(date'{min_d}', date'{max_d}', interval 1 day)) AS date")
    .withColumn("date_key",     F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("date"))
    .withColumn("quarter",      F.quarter("date"))
    .withColumn("month",        F.month("date"))
    .withColumn("month_name",   F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week",  F.dayofweek("date"))   # 1=Sun … 7=Sat
    .withColumn("day_name",     F.date_format("date", "EEEE"))
    .withColumn("is_weekend",   F.dayofweek("date").isin(1, 7))
    .select("date_key", "date", "year", "quarter", "month", "month_name",
            "week_of_year", "day_of_week", "day_name", "is_weekend")
)

write_dim(dim_date, "dim_date")

# ─────────────────────────────────────────────────────────────────────────────
# dim_batch
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_batch
# MAGIC Descriptive attributes for every batch run.
# MAGIC `start_date_key` and `end_date_key` are foreign keys to `dim_date`.

# COMMAND ----------
print("dim_batch — from bronze.batch_manifest")

dim_batch = (
    spark.table(btbl("batch_manifest"))
    .select(
        "batch_id", "scale_level", "equipment_id", "strain_id",
        "volume_l", "planned_duration_h", "actual_duration_h",
        "status", "failure_reason", "facility", "product",
        "start_time", "end_time",
        F.to_date("start_time").alias("start_date"),
        F.to_date("end_time").alias("end_date"),
        F.date_format("start_time", "yyyyMMdd").cast("int").alias("start_date_key"),
        F.date_format("end_time",   "yyyyMMdd").cast("int").alias("end_date_key"),
        F.year("start_time").alias("batch_year"),
        F.quarter("start_time").alias("batch_quarter"),
        F.month("start_time").alias("batch_month"),
    )
)

write_dim(dim_batch, "dim_batch")

# ─────────────────────────────────────────────────────────────────────────────
# dim_strain
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_strain
# MAGIC Reb-A cell factory engineering lineage.
# MAGIC `improvement_over_parent_pct` tracks the BioOptimization Cycle iteration gains.

# COMMAND ----------
print("dim_strain — from silver.sat_strain_engineering")

dim_strain = (
    spark.table(stbl("sat_strain_engineering"))
    .filter(F.col("load_end_ts").isNull())   # current records only
    .select(
        "strain_id", "parent_strain_id", "target_molecule", "chassis_organism",
        "engineering_modification", "expected_yield_g_l",
        "improvement_over_parent_pct", "approved_for_scale",
        "lead_scientist", "notes", "created_at",
    )
)

write_dim(dim_strain, "dim_strain")

# ─────────────────────────────────────────────────────────────────────────────
# dim_equipment
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_equipment
# MAGIC Canonical equipment metadata — bioreactors at all three scales plus downstream units.

# COMMAND ----------
print("dim_equipment — from bronze.equipment_mapping")

dim_equipment = spark.table(btbl("equipment_mapping"))   # already clean reference table

write_dim(dim_equipment, "dim_equipment")

# ─────────────────────────────────────────────────────────────────────────────
# dim_material
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_material
# MAGIC Raw material reference — carbon sources, nitrogen sources, pH agents, calibration standards.

# COMMAND ----------
print("dim_material — from bronze.material_mapping")

dim_material = spark.table(btbl("material_mapping"))     # already clean reference table

write_dim(dim_material, "dim_material")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Dimension Summary

# COMMAND ----------
print("\nDimension row counts:")
for d in ["dim_date", "dim_batch", "dim_strain", "dim_equipment", "dim_material"]:
    n = spark.table(gtbl(d)).count()
    print(f"  {d:<28} {n:>8,} rows")

# COMMAND ----------
display(spark.table(gtbl("dim_date")).orderBy("date_key").limit(7))

# COMMAND ----------
display(spark.table(gtbl("dim_strain")).orderBy("strain_id").limit(10))
