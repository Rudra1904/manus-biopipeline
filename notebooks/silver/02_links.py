# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver — Data Vault 2.0: Links
# MAGIC
# MAGIC Reads from `workspace.bronze.*` to establish relationships between business entities.
# MAGIC Hash keys are computed independently (same MD5 formula as hubs — no join to hub tables required).
# MAGIC Writes to `workspace.silver.*`.
# MAGIC
# MAGIC | Link | Relationship | Source |
# MAGIC |---|---|---|
# MAGIC | `lnk_batch_equipment` | Which bioreactor ran each batch | `bronze.batch_manifest` |
# MAGIC | `lnk_batch_strain` | Which engineered strain was used per batch | `bronze.batch_manifest` |
# MAGIC | `lnk_batch_material` | Which raw materials were available during each batch window | `bronze.batch_manifest` + `bronze.inventory_snapshots` |
# MAGIC
# MAGIC **Schema per link**: `{link}_hk | hub_a_hk | hub_b_hk | load_ts | record_source`

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

CATALOG = "workspace"
SILVER  = "silver"
BRONZE  = "bronze"

def stbl(name): return f"{CATALOG}.{SILVER}.{name}"
def btbl(name): return f"{CATALOG}.{BRONZE}.{name}"

def make_hk(*key_cols: str) -> F.Column:
    """MD5 hash key — upper + trim concatenation of business key columns."""
    return F.md5(F.concat_ws("||", *[F.upper(F.trim(F.col(c))) for c in key_cols]))

def write_link(df: DataFrame, name: str) -> None:
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(stbl(name)))
    n = spark.table(stbl(name)).count()
    print(f"  [OK] {name:<35} {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# lnk_batch_equipment
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## lnk_batch_equipment
# MAGIC Links each batch run to the bioreactor (or other equipment) it was executed on.
# MAGIC Source: `bronze.batch_manifest` — `(batch_id, equipment_id)` pairs are already 1:1 per batch.

# COMMAND ----------
print("lnk_batch_equipment")

lnk_batch_equipment = (
    spark.table(btbl("batch_manifest"))
    .select("batch_id", "equipment_id")
    .distinct()
    .withColumn("hub_batch_hk",           make_hk("batch_id"))
    .withColumn("hub_equipment_hk",       make_hk("equipment_id"))
    .withColumn("lnk_batch_equipment_hk", make_hk("batch_id", "equipment_id"))
    .withColumn("load_ts",                F.current_timestamp())
    .withColumn("record_source",          F.lit("bronze.batch_manifest"))
    .select("lnk_batch_equipment_hk", "hub_batch_hk", "hub_equipment_hk",
            "load_ts", "record_source")
)

write_link(lnk_batch_equipment, "lnk_batch_equipment")

# COMMAND ----------
# MAGIC %md
# MAGIC ## lnk_batch_strain
# MAGIC Links each batch run to the engineered Reb-A cell factory strain used.
# MAGIC Source: `bronze.batch_manifest` — `(batch_id, strain_id)` pairs track the BioOptimization Cycle.

# COMMAND ----------
print("lnk_batch_strain")

lnk_batch_strain = (
    spark.table(btbl("batch_manifest"))
    .select("batch_id", "strain_id")
    .distinct()
    .withColumn("hub_batch_hk",       make_hk("batch_id"))
    .withColumn("hub_strain_hk",      make_hk("strain_id"))
    .withColumn("lnk_batch_strain_hk", make_hk("batch_id", "strain_id"))
    .withColumn("load_ts",            F.current_timestamp())
    .withColumn("record_source",      F.lit("bronze.batch_manifest"))
    .select("lnk_batch_strain_hk", "hub_batch_hk", "hub_strain_hk",
            "load_ts", "record_source")
)

write_link(lnk_batch_strain, "lnk_batch_strain")

# ─────────────────────────────────────────────────────────────────────────────
# lnk_batch_material
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## lnk_batch_material
# MAGIC Links each batch to raw materials whose inventory was tracked during the batch window.
# MAGIC
# MAGIC Derived by joining `bronze.batch_manifest` batch date ranges with
# MAGIC `bronze.inventory_snapshots` snapshot dates — a snapshot date that falls within
# MAGIC `[batch.start_time::date, batch.end_time::date]` establishes the link.
# MAGIC Taking `DISTINCT (batch_id, material_id)` collapses daily overlaps into one link per pair.
# MAGIC Expected output: 672 batches × 7 materials = **4,704 links**.

# COMMAND ----------
print("lnk_batch_material")

batch_windows = (
    spark.table(btbl("batch_manifest"))
    .select(
        "batch_id",
        F.to_date("start_time").alias("batch_start_date"),
        F.to_date("end_time").alias("batch_end_date"),
    )
)

inv_dates = (
    spark.table(btbl("inventory_snapshots"))
    .select("material_id", "snapshot_date")
    .distinct()
)

lnk_batch_material = (
    batch_windows
    .join(
        inv_dates,
        (inv_dates["snapshot_date"] >= batch_windows["batch_start_date"]) &
        (inv_dates["snapshot_date"] <= batch_windows["batch_end_date"]),
        how="inner",
    )
    .select("batch_id", "material_id")
    .distinct()
    .withColumn("hub_batch_hk",          make_hk("batch_id"))
    .withColumn("hub_material_hk",       make_hk("material_id"))
    .withColumn("lnk_batch_material_hk", make_hk("batch_id", "material_id"))
    .withColumn("load_ts",               F.current_timestamp())
    .withColumn("record_source",         F.lit("bronze.batch_manifest+inventory_snapshots"))
    .select("lnk_batch_material_hk", "hub_batch_hk", "hub_material_hk",
            "load_ts", "record_source")
)

write_link(lnk_batch_material, "lnk_batch_material")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Link Summary

# COMMAND ----------
print("\nLink row counts:")
for lnk in ["lnk_batch_equipment", "lnk_batch_strain", "lnk_batch_material"]:
    n = spark.table(stbl(lnk)).count()
    print(f"  {lnk:<35} {n:>8,} rows")

# COMMAND ----------
# Verify referential integrity — every link hub_batch_hk must exist in hub_batch
for lnk_name, hk_col in [
    ("lnk_batch_equipment", "hub_batch_hk"),
    ("lnk_batch_strain",    "hub_batch_hk"),
    ("lnk_batch_material",  "hub_batch_hk"),
]:
    hub_keys = spark.table(stbl("hub_batch")).select("hub_batch_hk")
    orphans  = (spark.table(stbl(lnk_name))
                .join(hub_keys, on="hub_batch_hk", how="left_anti")
                .count())
    status = "OK" if orphans == 0 else f"WARN — {orphans} orphan keys"
    print(f"  {lnk_name:<35} batch_hk integrity: {status}")

# COMMAND ----------
display(spark.table(stbl("lnk_batch_equipment")).limit(5))

# COMMAND ----------
display(spark.table(stbl("lnk_batch_material")).limit(10))
