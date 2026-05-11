# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold — Star Schema: Facts
# MAGIC
# MAGIC Reads from `workspace.silver.*` satellites and `workspace.bronze.batch_manifest`.
# MAGIC Writes to `workspace.gold.*`.
# MAGIC
# MAGIC | Fact | Grain | Measures |
# MAGIC |---|---|---|
# MAGIC | `fact_batch_run` | One row per batch | Sensor KPIs + QC results + batch status |
# MAGIC | `fact_scale_translation` | One row per (strain × scale) | Aggregated titer / yield / pass rate — powers the Valley of Death chart |

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

def write_fact(df: DataFrame, name: str) -> None:
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(gtbl(name)))
    n = spark.table(gtbl(name)).count()
    print(f"  [OK] {name:<30} {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# fact_batch_run
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## fact_batch_run
# MAGIC
# MAGIC Central fact table — one row per batch run.
# MAGIC Joins batch context (dates, scale, equipment, strain) with aggregated sensor KPIs
# MAGIC and LIMS QC results from the Silver satellite layer.
# MAGIC
# MAGIC **Foreign keys**: `batch_id → dim_batch`, `equipment_id → dim_equipment`,
# MAGIC `strain_id → dim_strain`, `start/end_date_key → dim_date`

# COMMAND ----------
print("fact_batch_run — joining batch_manifest + sat_batch_sensors + sat_batch_qc")

batch_ctx = (
    spark.table(btbl("batch_manifest"))
    .select(
        "batch_id", "scale_level", "equipment_id", "strain_id",
        "status", "failure_reason", "volume_l",
        "planned_duration_h", "actual_duration_h",
        "facility", "product",
        F.to_date("start_time").alias("start_date"),
        F.to_date("end_time").alias("end_date"),
        F.date_format("start_time", "yyyyMMdd").cast("int").alias("start_date_key"),
        F.date_format("end_time",   "yyyyMMdd").cast("int").alias("end_date_key"),
    )
)

sensors = (
    spark.table(stbl("sat_batch_sensors"))
    .select(
        "batch_id",
        "sensor_row_count", "batch_duration_h",
        "avg_temperature_c", "avg_ph", "avg_dissolved_o2_pct",
        "avg_glucose_g_l", "avg_biomass_od600", "avg_co2_evolution_rate",
        "avg_reba_titer_g_l", "max_reba_titer_g_l",
        "first_reading_ts", "last_reading_ts",
    )
)

qc = (
    spark.table(stbl("sat_batch_qc"))
    .select(
        "batch_id",
        "sample_count", "pass_count", "fail_count", "pending_count",
        "avg_purity_pct", "min_purity_pct", "avg_yield_g_l",
        "avg_viability_pct", "avg_endotoxin_eu_ml", "max_endotoxin_eu_ml",
        "endotoxin_breach_count", "batch_qc_status",
    )
)

fact_batch_run = (
    batch_ctx
    .join(sensors, on="batch_id", how="left")
    .join(qc,      on="batch_id", how="left")
)

write_fact(fact_batch_run, "fact_batch_run")

# ─────────────────────────────────────────────────────────────────────────────
# fact_scale_translation
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## fact_scale_translation
# MAGIC
# MAGIC Aggregated fact: one row per `(strain_id, scale_level)` combination.
# MAGIC Answers: *"How does this strain's Reb-A titer and yield change from lab → pilot → manufacturing?"*
# MAGIC This is the **Valley of Death** visualisation source — the core portfolio KPI.
# MAGIC
# MAGIC Foreign keys: `strain_id → dim_strain`, `scale_level` (denormalized filter)

# COMMAND ----------
print("fact_scale_translation — aggregating fact_batch_run by (strain_id, scale_level)")

fact_scale_translation = (
    spark.table(gtbl("fact_batch_run"))
    .groupBy("strain_id", "scale_level")
    .agg(
        F.count("batch_id")                                              .alias("batch_count"),
        F.round(F.avg("avg_reba_titer_g_l"),  4)                        .alias("avg_reba_titer_g_l"),
        F.round(F.max("max_reba_titer_g_l"),  4)                        .alias("peak_reba_titer_g_l"),
        F.round(F.avg("avg_yield_g_l"),        4)                        .alias("avg_yield_g_l"),
        F.round(F.avg("avg_purity_pct"),       3)                        .alias("avg_purity_pct"),
        F.round(F.avg("avg_endotoxin_eu_ml"),  4)                        .alias("avg_endotoxin_eu_ml"),
        F.round(F.avg("batch_duration_h"),     1)                        .alias("avg_batch_duration_h"),
        F.round(F.avg("avg_glucose_g_l"),      3)                        .alias("avg_glucose_g_l"),
        F.round(F.avg("avg_dissolved_o2_pct"), 3)                        .alias("avg_dissolved_o2_pct"),
        F.round(
            F.sum((F.col("batch_qc_status") == "PASS").cast("int")) /
            F.count("batch_id") * 100, 2
        )                                                                .alias("pass_rate_pct"),
        F.sum((F.col("status") == "FAILED").cast("int"))                 .alias("failed_batch_count"),
    )
    .withColumn(
        "scale_order",
        F.when(F.col("scale_level") == "lab",           F.lit(1))
         .when(F.col("scale_level") == "pilot",         F.lit(2))
         .when(F.col("scale_level") == "manufacturing", F.lit(3))
         .otherwise(F.lit(9))
    )
    .orderBy("strain_id", "scale_order")
)

write_fact(fact_scale_translation, "fact_scale_translation")

# ─────────────────────────────────────────────────────────────────────────────
# Summary + Spot-checks
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Fact Summary

# COMMAND ----------
print("\nFact row counts:")
for f in ["fact_batch_run", "fact_scale_translation"]:
    n = spark.table(gtbl(f)).count()
    print(f"  {f:<30} {n:>8,} rows")

# COMMAND ----------
# Spot-check: batch status breakdown
display(
    spark.table(gtbl("fact_batch_run"))
    .groupBy("scale_level", "status")
    .agg(
        F.count("batch_id").alias("batch_count"),
        F.round(F.avg("avg_reba_titer_g_l"), 4).alias("avg_reba_titer"),
        F.round(F.avg("avg_yield_g_l"),       4).alias("avg_yield_g_l"),
    )
    .orderBy("scale_level", "status")
)

# COMMAND ----------
# Spot-check: Valley of Death — top 10 strains that have data at all 3 scales
display(
    spark.table(gtbl("fact_scale_translation"))
    .join(
        spark.table(gtbl("fact_scale_translation"))
             .groupBy("strain_id").count().filter(F.col("count") == 3)
             .select("strain_id"),
        on="strain_id"
    )
    .join(spark.table(gtbl("dim_strain")).select("strain_id", "engineering_modification"),
          on="strain_id", how="left")
    .select("strain_id", "engineering_modification", "scale_level", "scale_order",
            "batch_count", "avg_reba_titer_g_l", "peak_reba_titer_g_l",
            "avg_yield_g_l", "pass_rate_pct")
    .orderBy("strain_id", "scale_order")
    .limit(30)
)
