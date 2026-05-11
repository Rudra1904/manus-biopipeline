# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver — Data Vault 2.0: Satellites
# MAGIC
# MAGIC Reads from `workspace.bronze.*` clean views and reference tables.
# MAGIC Computes descriptive attributes for each hub entity and writes to `workspace.silver.*`.
# MAGIC `load_end_ts = NULL` indicates the current/active record (full-overwrite pipeline).
# MAGIC
# MAGIC | Satellite | Hub | Grain | Source |
# MAGIC |---|---|---|---|
# MAGIC | `sat_batch_sensors` | hub_batch | One row per batch — aggregated sensor stats | `bronze.bioreactor_sensors_clean` |
# MAGIC | `sat_batch_qc` | hub_batch | One row per batch — aggregated QC results | `bronze.lims_results_clean` |
# MAGIC | `sat_equipment_maintenance` | hub_equipment | One row per equipment — aggregated WO stats | `bronze.work_orders_clean` |
# MAGIC | `sat_strain_engineering` | hub_strain | One row per strain — engineering lineage attrs | `bronze.strain_registry` |
# MAGIC | `sat_material_inventory` | hub_material | One row per material — latest snapshot + stats | `bronze.inventory_snapshots` |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType

CATALOG = "workspace"
SILVER  = "silver"
BRONZE  = "bronze"

def stbl(name): return f"{CATALOG}.{SILVER}.{name}"
def btbl(name): return f"{CATALOG}.{BRONZE}.{name}"

def make_hk(*key_cols: str) -> F.Column:
    """MD5 hash key — upper + trim concatenation of business key columns."""
    return F.md5(F.concat_ws("||", *[F.upper(F.trim(F.col(c))) for c in key_cols]))

def make_hash_diff(*attr_cols: str) -> F.Column:
    """MD5 hash diff — coalesced string of attribute columns for change detection."""
    return F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in attr_cols]))

def write_sat(df: DataFrame, name: str) -> None:
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(stbl(name)))
    n = spark.table(stbl(name)).count()
    print(f"  [OK] {name:<35} {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# sat_batch_sensors
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## sat_batch_sensors
# MAGIC
# MAGIC Aggregates 88.9 M sensor readings (2-second cadence) to one row per batch.
# MAGIC Key metrics: avg/max Reb-A titer (g/L), temperature, pH, DO%, glucose, OD600.
# MAGIC `max_reba_titer_g_l` is the primary BioOptimization Cycle KPI.

# COMMAND ----------
print("sat_batch_sensors — aggregating 88.9 M sensor rows by batch_id...")

SENSOR_ATTR_COLS = [
    "avg_temperature_c", "avg_ph", "avg_dissolved_o2_pct",
    "avg_glucose_g_l", "avg_biomass_od600", "avg_co2_evolution_rate",
    "avg_reba_titer_g_l", "max_reba_titer_g_l",
]

sensors_agg = (
    spark.table(btbl("bioreactor_sensors_clean"))
    .groupBy("batch_id", "scale_level", "equipment_id")
    .agg(
        F.count("*")                                   .alias("sensor_row_count"),
        F.round(F.avg("temperature_c"),    3)          .alias("avg_temperature_c"),
        F.round(F.avg("ph"),               3)          .alias("avg_ph"),
        F.round(F.avg("dissolved_o2_pct"), 3)          .alias("avg_dissolved_o2_pct"),
        F.round(F.avg("glucose_g_l"),      3)          .alias("avg_glucose_g_l"),
        F.round(F.avg("biomass_od600"),    4)          .alias("avg_biomass_od600"),
        F.round(F.avg("co2_evolution_rate"), 4)        .alias("avg_co2_evolution_rate"),
        F.round(F.avg("reba_titer_g_l"),   4)          .alias("avg_reba_titer_g_l"),
        F.round(F.max("reba_titer_g_l"),   4)          .alias("max_reba_titer_g_l"),
        F.min("timestamp")                             .alias("first_reading_ts"),
        F.max("timestamp")                             .alias("last_reading_ts"),
    )
    .withColumn(
        "batch_duration_h",
        F.round(
            (F.unix_timestamp("last_reading_ts") - F.unix_timestamp("first_reading_ts")) / 3600,
            2
        )
    )
)

sat_batch_sensors = (
    sensors_agg
    .withColumn("hub_batch_hk",  make_hk("batch_id"))
    .withColumn("load_ts",       F.current_timestamp())
    .withColumn("load_end_ts",   F.lit(None).cast(TimestampType()))
    .withColumn("hash_diff",     make_hash_diff(*SENSOR_ATTR_COLS))
    .withColumn("record_source", F.lit("bronze.bioreactor_sensors_clean"))
    .select(
        "hub_batch_hk", "load_ts", "load_end_ts", "hash_diff", "record_source",
        "batch_id", "scale_level", "equipment_id",
        "sensor_row_count",
        "avg_temperature_c", "avg_ph", "avg_dissolved_o2_pct",
        "avg_glucose_g_l", "avg_biomass_od600", "avg_co2_evolution_rate",
        "avg_reba_titer_g_l", "max_reba_titer_g_l",
        "first_reading_ts", "last_reading_ts", "batch_duration_h",
    )
)

write_sat(sat_batch_sensors, "sat_batch_sensors")

# ─────────────────────────────────────────────────────────────────────────────
# sat_batch_qc
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## sat_batch_qc
# MAGIC
# MAGIC Aggregates LIMS QC results (3 samples per batch — early IPC, mid IPC, end-of-batch).
# MAGIC `batch_qc_status`: PASS = all samples pass; PARTIAL = mix; FAIL = all fail.
# MAGIC cGMP release threshold: `max_endotoxin_eu_ml < 1.0 EU/mL`.

# COMMAND ----------
print("sat_batch_qc — aggregating LIMS results by batch_id...")

QC_ATTR_COLS = [
    "sample_count", "pass_count", "fail_count",
    "avg_purity_pct", "min_purity_pct", "avg_yield_g_l",
    "avg_viability_pct", "avg_endotoxin_eu_ml", "max_endotoxin_eu_ml",
    "endotoxin_breach_count", "batch_qc_status",
]

lims_agg = (
    spark.table(btbl("lims_results_clean"))
    .groupBy("batch_id")
    .agg(
        F.count("*")                                      .alias("sample_count"),
        F.sum((F.col("status") == "PASS").cast("int"))    .alias("pass_count"),
        F.sum((F.col("status") == "FAIL").cast("int"))    .alias("fail_count"),
        F.sum((F.col("status") == "PENDING").cast("int")) .alias("pending_count"),
        F.round(F.avg("purity_pct"),      3)              .alias("avg_purity_pct"),
        F.round(F.min("purity_pct"),      3)              .alias("min_purity_pct"),
        F.round(F.avg("yield_g_l"),       4)              .alias("avg_yield_g_l"),
        F.round(F.avg("viability_pct"),   3)              .alias("avg_viability_pct"),
        F.round(F.avg("endotoxin_eu_ml"), 4)              .alias("avg_endotoxin_eu_ml"),
        F.round(F.max("endotoxin_eu_ml"), 4)              .alias("max_endotoxin_eu_ml"),
        F.sum((F.col("endotoxin_eu_ml") > 1.0).cast("int")).alias("endotoxin_breach_count"),
    )
    .withColumn(
        "batch_qc_status",
        F.when(F.col("fail_count") == 0, F.lit("PASS"))
         .when(F.col("pass_count") == 0, F.lit("FAIL"))
         .otherwise(F.lit("PARTIAL"))
    )
)

sat_batch_qc = (
    lims_agg
    .withColumn("hub_batch_hk",  make_hk("batch_id"))
    .withColumn("load_ts",       F.current_timestamp())
    .withColumn("load_end_ts",   F.lit(None).cast(TimestampType()))
    .withColumn("hash_diff",     make_hash_diff(*QC_ATTR_COLS))
    .withColumn("record_source", F.lit("bronze.lims_results_clean"))
    .select(
        "hub_batch_hk", "load_ts", "load_end_ts", "hash_diff", "record_source",
        "batch_id",
        "sample_count", "pass_count", "fail_count", "pending_count",
        "avg_purity_pct", "min_purity_pct", "avg_yield_g_l",
        "avg_viability_pct", "avg_endotoxin_eu_ml", "max_endotoxin_eu_ml",
        "endotoxin_breach_count", "batch_qc_status",
    )
)

write_sat(sat_batch_qc, "sat_batch_qc")

# ─────────────────────────────────────────────────────────────────────────────
# sat_equipment_maintenance
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## sat_equipment_maintenance
# MAGIC
# MAGIC Aggregates CMMS work orders per equipment unit.
# MAGIC Tracks: WO counts by type, total downtime (h), labor, parts cost, and next scheduled date.
# MAGIC cGMP note: manufacturing bioreactor `BR-MFG-01` requires weekly Preventive Maintenance.

# COMMAND ----------
print("sat_equipment_maintenance — aggregating work orders by equipment_id...")

WO_ATTR_COLS = [
    "total_wo_count", "preventive_count", "corrective_count",
    "predictive_count", "emergency_count",
    "total_downtime_hours", "total_labor_hours", "total_parts_cost_usd",
]

wo_agg = (
    spark.table(btbl("work_orders_clean"))
    .groupBy("equipment_id")
    .agg(
        F.count("*")                                                .alias("total_wo_count"),
        F.sum((F.col("wo_type") == "Preventive").cast("int"))       .alias("preventive_count"),
        F.sum((F.col("wo_type") == "Corrective").cast("int"))       .alias("corrective_count"),
        F.sum((F.col("wo_type") == "Predictive").cast("int"))       .alias("predictive_count"),
        F.sum((F.col("wo_type") == "Emergency").cast("int"))        .alias("emergency_count"),
        F.round(F.sum("downtime_hours"), 2)                         .alias("total_downtime_hours"),
        F.round(F.sum("labor_hours"),    2)                         .alias("total_labor_hours"),
        F.round(F.sum("parts_cost_usd"), 2)                         .alias("total_parts_cost_usd"),
        F.max("completed_at")                                       .alias("last_wo_completed_at"),
        # Next open WO: min scheduled_date where work is not yet completed
        F.min(F.when(F.col("completed_at").isNull(), F.col("scheduled_date")))
                                                                    .alias("next_scheduled_date"),
    )
)

sat_equipment_maintenance = (
    wo_agg
    .withColumn("hub_equipment_hk", make_hk("equipment_id"))
    .withColumn("load_ts",          F.current_timestamp())
    .withColumn("load_end_ts",      F.lit(None).cast(TimestampType()))
    .withColumn("hash_diff",        make_hash_diff(*WO_ATTR_COLS))
    .withColumn("record_source",    F.lit("bronze.work_orders_clean"))
    .select(
        "hub_equipment_hk", "load_ts", "load_end_ts", "hash_diff", "record_source",
        "equipment_id",
        "total_wo_count", "preventive_count", "corrective_count",
        "predictive_count", "emergency_count",
        "total_downtime_hours", "total_labor_hours", "total_parts_cost_usd",
        "last_wo_completed_at", "next_scheduled_date",
    )
)

write_sat(sat_equipment_maintenance, "sat_equipment_maintenance")

# ─────────────────────────────────────────────────────────────────────────────
# sat_strain_engineering
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## sat_strain_engineering
# MAGIC
# MAGIC Descriptive attributes for each Reb-A cell factory strain.
# MAGIC Tracks lineage (`parent_strain_id`), chassis organism, engineering modification,
# MAGIC expected yield improvement over parent, and scale approval status.
# MAGIC This satellite powers the BioOptimization Cycle tracker in the Gold/dashboard layer.

# COMMAND ----------
print("sat_strain_engineering — from bronze.strain_registry (100 strains)...")

STRAIN_ATTR_COLS = [
    "parent_strain_id", "target_molecule", "chassis_organism",
    "engineering_modification", "expected_yield_g_l",
    "improvement_over_parent_pct", "approved_for_scale",
    "lead_scientist",
]

sat_strain_engineering = (
    spark.table(btbl("strain_registry"))
    .withColumn("hub_strain_hk",  make_hk("strain_id"))
    .withColumn("load_ts",        F.current_timestamp())
    .withColumn("load_end_ts",    F.lit(None).cast(TimestampType()))
    .withColumn("hash_diff",      make_hash_diff(*STRAIN_ATTR_COLS))
    .withColumn("record_source",  F.lit("bronze.strain_registry"))
    .select(
        "hub_strain_hk", "load_ts", "load_end_ts", "hash_diff", "record_source",
        "strain_id",
        "parent_strain_id", "target_molecule", "chassis_organism",
        "engineering_modification", "expected_yield_g_l",
        "improvement_over_parent_pct", "approved_for_scale",
        "lead_scientist", "notes", "created_at",
    )
)

write_sat(sat_strain_engineering, "sat_strain_engineering")

# ─────────────────────────────────────────────────────────────────────────────
# sat_material_inventory
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## sat_material_inventory
# MAGIC
# MAGIC Current inventory state (most recent snapshot) plus supply risk stats
# MAGIC aggregated over all 182 daily snapshots.
# MAGIC `days_below_reorder`: days where stock fell below reorder point — key supply risk KPI.

# COMMAND ----------
print("sat_material_inventory — latest snapshot per material + risk stats...")

INV_ATTR_COLS = [
    "quantity_on_hand", "reorder_point", "max_stock",
    "below_reorder_point", "days_of_supply",
    "days_below_reorder", "total_snapshot_days",
]

# Window: most recent snapshot per material
latest_w = Window.partitionBy("material_id").orderBy(F.desc("snapshot_date"))

latest_inv = (
    spark.table(btbl("inventory_snapshots"))
    .withColumn("_rn", F.row_number().over(latest_w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingested_at", "_source", "_source_file")
)

# Aggregate supply risk stats across all snapshots
inv_stats = (
    spark.table(btbl("inventory_snapshots"))
    .groupBy("material_id")
    .agg(
        F.sum(F.col("below_reorder_point").cast("int")).alias("days_below_reorder"),
        F.count("*")                                   .alias("total_snapshot_days"),
        F.round(F.avg("quantity_on_hand"), 1)          .alias("avg_qty_on_hand"),
        F.round(F.avg("days_of_supply"),   1)          .alias("avg_days_of_supply"),
    )
)

# Join material_mapping for category + unit_standard
mat_map = spark.table(btbl("material_mapping"))

sat_material_inventory = (
    latest_inv
    .join(mat_map.select("material_id", "unit_standard", "category"), on="material_id", how="left")
    .join(inv_stats, on="material_id", how="left")
    .withColumn("hub_material_hk", make_hk("material_id"))
    .withColumn("load_ts",         F.current_timestamp())
    .withColumn("load_end_ts",     F.lit(None).cast(TimestampType()))
    .withColumn("hash_diff",       make_hash_diff(*INV_ATTR_COLS))
    .withColumn("record_source",   F.lit("bronze.inventory_snapshots"))
    .select(
        "hub_material_hk", "load_ts", "load_end_ts", "hash_diff", "record_source",
        "material_id", "material_name", "unit_standard", "category",
        "snapshot_date",
        "quantity_on_hand", "reorder_point", "max_stock",
        "below_reorder_point", "days_of_supply",
        "supplier_id", "last_receipt_date", "last_receipt_qty",
        "days_below_reorder", "total_snapshot_days",
        "avg_qty_on_hand", "avg_days_of_supply",
    )
)

write_sat(sat_material_inventory, "sat_material_inventory")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Satellite Summary

# COMMAND ----------
print("\nSatellite row counts:")
sats = [
    "sat_batch_sensors", "sat_batch_qc",
    "sat_equipment_maintenance", "sat_strain_engineering", "sat_material_inventory",
]
for sat in sats:
    n = spark.table(stbl(sat)).count()
    print(f"  {sat:<35} {n:>8,} rows")

# COMMAND ----------
# Spot-check: avg Reb-A titer by scale level — the BioOptimization Cycle signal
display(
    spark.table(stbl("sat_batch_sensors"))
    .groupBy("scale_level")
    .agg(
        F.count("batch_id").alias("batch_count"),
        F.round(F.avg("avg_reba_titer_g_l"), 4).alias("mean_avg_titer_g_l"),
        F.round(F.avg("max_reba_titer_g_l"), 4).alias("mean_peak_titer_g_l"),
        F.round(F.avg("batch_duration_h"),   1).alias("mean_batch_duration_h"),
    )
    .orderBy("scale_level")
)

# COMMAND ----------
# Spot-check: QC pass rate by batch_qc_status
display(
    spark.table(stbl("sat_batch_qc"))
    .groupBy("batch_qc_status")
    .agg(
        F.count("*").alias("batch_count"),
        F.round(F.avg("avg_purity_pct"),      2).alias("avg_purity_pct"),
        F.round(F.avg("avg_yield_g_l"),        3).alias("avg_yield_g_l"),
        F.round(F.avg("max_endotoxin_eu_ml"),  4).alias("avg_max_endotoxin_eu_ml"),
    )
    .orderBy("batch_qc_status")
)

# COMMAND ----------
# Spot-check: equipment downtime leaderboard
display(
    spark.table(stbl("sat_equipment_maintenance"))
    .join(spark.table("workspace.bronze.equipment_mapping").select("equipment_id", "full_name", "area"),
          on="equipment_id", how="left")
    .select("equipment_id", "full_name", "area",
            "total_wo_count", "total_downtime_hours", "total_parts_cost_usd")
    .orderBy(F.desc("total_downtime_hours"))
)

# COMMAND ----------
# Spot-check: supply risk — materials most frequently below reorder point
display(
    spark.table(stbl("sat_material_inventory"))
    .select("material_name", "category", "unit_standard",
            "days_below_reorder", "total_snapshot_days",
            F.round(F.col("days_below_reorder") / F.col("total_snapshot_days") * 100, 1).alias("pct_days_below_reorder"),
            "avg_days_of_supply", "quantity_on_hand", "below_reorder_point")
    .orderBy(F.desc("days_below_reorder"))
)
