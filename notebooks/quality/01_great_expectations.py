# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Data Quality — Great Expectations Style Checks
# MAGIC
# MAGIC PySpark implementation of GE-style expectations across Bronze and Silver layers.
# MAGIC Results written to `workspace.bronze.dq_great_expectations_report`.
# MAGIC
# MAGIC | Suite | Table | Checks |
# MAGIC |---|---|---|
# MAGIC | `bronze_bioreactor_sensors` | `bronze.bioreactor_sensors` | pH range, temp range, batch_id/timestamp not null |
# MAGIC | `bronze_lims_results` | `bronze.lims_results` | purity range, status set, batch_id not null |
# MAGIC | `bronze_batch_manifest` | `bronze.batch_manifest` | status set, scale_level set |
# MAGIC | `silver_hub_batch` | `silver.hub_batch` | batch_hk not null, batch_id unique |
# MAGIC | `silver_hub_strain` | `silver.hub_strain` | strain_hk not null, strain_id unique |
# MAGIC | `silver_sat_batch_sensors_summary` | `silver.sat_batch_sensors` | peak_reba_titer_g_l >= 0, sensor_reading_count > 0 |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType, TimestampType
)
from datetime import datetime

CATALOG = "workspace"
BRONZE  = "bronze"
SILVER  = "silver"

def btbl(n): return f"{CATALOG}.{BRONZE}.{n}"
def stbl(n): return f"{CATALOG}.{SILVER}.{n}"

REPORT_TABLE = btbl("dq_great_expectations_report")

# Accumulator for expectation results
results = []

def record(suite_name: str, expectation: str, column: str,
           success: bool, unexpected_count: int) -> None:
    results.append({
        "suite_name":        suite_name,
        "expectation":       expectation,
        "column":            column,
        "success":           success,
        "unexpected_count":  unexpected_count,
        "run_timestamp":     datetime.utcnow(),
    })

# ─────────────────────────────────────────────────────────────────────────────
# Helper expectations
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
def expect_column_values_to_not_be_null(df, suite, col_name, report_col=None):
    rc = report_col or col_name
    n  = df.filter(F.col(col_name).isNull()).count()
    record(suite, "expect_column_values_to_not_be_null", rc, n == 0, n)

def expect_column_values_to_be_between(df, suite, col_name, min_val, max_val, report_col=None):
    rc = report_col or col_name
    n  = df.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).between(min_val, max_val)
         ).count()
    record(suite, f"expect_column_values_to_be_between[{min_val},{max_val}]", rc, n == 0, n)

def expect_column_values_to_be_in_set(df, suite, col_name, value_set, report_col=None):
    rc = report_col or col_name
    n  = df.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).isin(value_set)
         ).count()
    record(suite, f"expect_column_values_to_be_in_set{sorted(value_set)}", rc, n == 0, n)

def expect_column_values_to_be_unique(df, suite, col_name, report_col=None):
    rc   = report_col or col_name
    total = df.count()
    distinct = df.select(col_name).distinct().count()
    dupes = total - distinct
    record(suite, "expect_column_values_to_be_unique", rc, dupes == 0, dupes)

def expect_column_values_to_be_gte(df, suite, col_name, threshold, report_col=None):
    rc = report_col or col_name
    n  = df.filter(
            F.col(col_name).isNotNull() &
            (F.col(col_name) < threshold)
         ).count()
    record(suite, f"expect_column_values_to_be_gte[{threshold}]", rc, n == 0, n)

def expect_column_values_to_be_gt(df, suite, col_name, threshold, report_col=None):
    rc = report_col or col_name
    n  = df.filter(
            F.col(col_name).isNotNull() &
            (F.col(col_name) <= threshold)
         ).count()
    record(suite, f"expect_column_values_to_be_gt[{threshold}]", rc, n == 0, n)

# ─────────────────────────────────────────────────────────────────────────────
# Suite 1 — bronze.bioreactor_sensors
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: bronze_bioreactor_sensors

# COMMAND ----------
SUITE = "bronze_bioreactor_sensors"
print(f"Running suite: {SUITE}")

sensors = spark.table(btbl("bioreactor_sensors"))

expect_column_values_to_not_be_null(sensors, SUITE, "batch_id")
expect_column_values_to_not_be_null(sensors, SUITE, "timestamp")
expect_column_values_to_be_between(sensors, SUITE, "ph",            6.0,  8.5)
expect_column_values_to_be_between(sensors, SUITE, "temperature_c", 30.0, 42.0)

print(f"  {len(results)} expectations checked so far")

# ─────────────────────────────────────────────────────────────────────────────
# Suite 2 — bronze.lims_results
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: bronze_lims_results

# COMMAND ----------
SUITE = "bronze_lims_results"
print(f"Running suite: {SUITE}")
prev = len(results)

lims = spark.table(btbl("lims_results"))

expect_column_values_to_not_be_null(lims, SUITE, "batch_id")
expect_column_values_to_be_between(lims, SUITE, "purity_pct", 0.0, 100.0)
expect_column_values_to_be_in_set(
    lims, SUITE, "status",
    ["PASS", "FAIL", "CONDITIONAL", "PENDING"]
)

print(f"  {len(results) - prev} expectations checked")

# ─────────────────────────────────────────────────────────────────────────────
# Suite 3 — bronze.batch_manifest
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: bronze_batch_manifest

# COMMAND ----------
SUITE = "bronze_batch_manifest"
print(f"Running suite: {SUITE}")
prev = len(results)

manifest = spark.table(btbl("batch_manifest"))

expect_column_values_to_be_in_set(
    manifest, SUITE, "status",
    ["COMPLETED", "FAILED", "ABORTED"]
)
expect_column_values_to_be_in_set(
    manifest, SUITE, "scale_level",
    ["lab", "pilot", "manufacturing"]
)

print(f"  {len(results) - prev} expectations checked")

# ─────────────────────────────────────────────────────────────────────────────
# Suite 4 — silver.hub_batch
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: silver_hub_batch

# COMMAND ----------
SUITE = "silver_hub_batch"
print(f"Running suite: {SUITE}")
prev = len(results)

hub_batch = spark.table(stbl("hub_batch"))

# hub_batch_hk → reported as batch_hk
expect_column_values_to_not_be_null(hub_batch, SUITE, "hub_batch_hk",  report_col="batch_hk")
expect_column_values_to_be_unique(  hub_batch, SUITE, "batch_id",      report_col="batch_id")

print(f"  {len(results) - prev} expectations checked")

# ─────────────────────────────────────────────────────────────────────────────
# Suite 5 — silver.hub_strain
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: silver_hub_strain

# COMMAND ----------
SUITE = "silver_hub_strain"
print(f"Running suite: {SUITE}")
prev = len(results)

hub_strain = spark.table(stbl("hub_strain"))

# hub_strain_hk → reported as strain_hk
expect_column_values_to_not_be_null(hub_strain, SUITE, "hub_strain_hk", report_col="strain_hk")
expect_column_values_to_be_unique(  hub_strain, SUITE, "strain_id",     report_col="strain_id")

print(f"  {len(results) - prev} expectations checked")

# ─────────────────────────────────────────────────────────────────────────────
# Suite 6 — silver.sat_batch_sensors  (suite name: sat_batch_sensors_summary)
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Suite: silver_sat_batch_sensors_summary

# COMMAND ----------
SUITE = "silver_sat_batch_sensors_summary"
print(f"Running suite: {SUITE}")
prev = len(results)

sat_sensors = spark.table(stbl("sat_batch_sensors"))

# max_reba_titer_g_l → reported as peak_reba_titer_g_l
expect_column_values_to_be_gte(sat_sensors, SUITE, "max_reba_titer_g_l",
                                threshold=0.0, report_col="peak_reba_titer_g_l")
# sensor_row_count → reported as sensor_reading_count
expect_column_values_to_be_gt(sat_sensors, SUITE, "sensor_row_count",
                               threshold=0, report_col="sensor_reading_count")

print(f"  {len(results) - prev} expectations checked")

# ─────────────────────────────────────────────────────────────────────────────
# Write results to bronze.dq_great_expectations_report
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Report

# COMMAND ----------
schema = StructType([
    StructField("suite_name",       StringType(),    False),
    StructField("expectation",      StringType(),    False),
    StructField("column",           StringType(),    False),
    StructField("success",          BooleanType(),   False),
    StructField("unexpected_count", LongType(),      False),
    StructField("run_timestamp",    TimestampType(), False),
])

report_df = spark.createDataFrame(results, schema=schema)

(report_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REPORT_TABLE))

n = spark.table(REPORT_TABLE).count()
print(f"[OK] {REPORT_TABLE}   {n} rows written")

# ─────────────────────────────────────────────────────────────────────────────
# Pass / Fail Summary
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pass / Fail Summary

# COMMAND ----------
print("\n=== Great Expectations Report ===\n")
print(f"{'SUITE':<40} {'EXPECTATION':<55} {'COLUMN':<30} {'RESULT':<8} {'UNEXPECTED':>12}")
print("-" * 148)

report = spark.table(REPORT_TABLE).orderBy("suite_name", "column").collect()
passed = 0
failed = 0

for row in report:
    status = "PASS" if row.success else "FAIL"
    if row.success:
        passed += 1
    else:
        failed += 1
    print(f"{row.suite_name:<40} {row.expectation:<55} {row.column:<30} {status:<8} {row.unexpected_count:>12,}")

print("-" * 148)
print(f"\nTotal: {len(report)} expectations   PASS: {passed}   FAIL: {failed}")

# COMMAND ----------
# Suite-level summary
print("\n=== Suite Summary ===\n")
display(
    spark.table(REPORT_TABLE)
    .groupBy("suite_name")
    .agg(
        F.count("*").alias("total_checks"),
        F.sum(F.col("success").cast("int")).alias("passed"),
        (F.count("*") - F.sum(F.col("success").cast("int"))).alias("failed"),
        F.round(F.sum(F.col("success").cast("int")) / F.count("*") * 100, 1).alias("pass_rate_pct"),
        F.sum("unexpected_count").alias("total_unexpected_rows"),
    )
    .orderBy("suite_name")
)
