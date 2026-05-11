# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver — Data Vault 2.0: Hubs
# MAGIC
# MAGIC Reads from `workspace.bronze.*` (authoritative reference tables for business keys).
# MAGIC Writes to `workspace.silver.*`.
# MAGIC
# MAGIC | Hub | Business Key | Source |
# MAGIC |---|---|---|
# MAGIC | `hub_batch` | `batch_id` | `bronze.batch_manifest` |
# MAGIC | `hub_equipment` | `equipment_id` | `bronze.equipment_mapping` |
# MAGIC | `hub_strain` | `strain_id` | `bronze.strain_registry` |
# MAGIC | `hub_material` | `material_id` | `bronze.material_mapping` |
# MAGIC
# MAGIC **Schema per hub**: `{hub}_hk (MD5) | business_key | load_ts | record_source`

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

CATALOG = "workspace"
SILVER  = "silver"
BRONZE  = "bronze"

def stbl(name: str) -> str:  return f"{CATALOG}.{SILVER}.{name}"
def btbl(name: str) -> str:  return f"{CATALOG}.{BRONZE}.{name}"

# Ensure silver schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")
print(f"Schema {CATALOG}.{SILVER} ready.")

def make_hk(*key_cols: str) -> F.Column:
    """MD5 hash key — upper + trim concatenation of business key columns."""
    return F.md5(F.concat_ws("||", *[F.upper(F.trim(F.col(c))) for c in key_cols]))

def write_hub(df: DataFrame, name: str) -> None:
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(stbl(name)))
    n = spark.table(stbl(name)).count()
    print(f"  [OK] {name:<30} {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# hub_batch
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## hub_batch
# MAGIC Business key: `batch_id` — every fermentation run.
# MAGIC Source: `bronze.batch_manifest` (the spine of the pipeline — 672 unique batches).

# COMMAND ----------
print("hub_batch")

hub_batch = (
    spark.table(btbl("batch_manifest"))
    .select("batch_id")
    .distinct()
    .withColumn("hub_batch_hk",  make_hk("batch_id"))
    .withColumn("load_ts",       F.current_timestamp())
    .withColumn("record_source", F.lit("bronze.batch_manifest"))
    .select("hub_batch_hk", "batch_id", "load_ts", "record_source")
)

write_hub(hub_batch, "hub_batch")

# COMMAND ----------
# MAGIC %md
# MAGIC ## hub_equipment
# MAGIC Business key: `equipment_id` — bioreactors, centrifuges, and utility equipment.
# MAGIC Source: `bronze.equipment_mapping` (12 equipment units across lab / pilot / manufacturing).

# COMMAND ----------
print("hub_equipment")

hub_equipment = (
    spark.table(btbl("equipment_mapping"))
    .select("equipment_id")
    .distinct()
    .withColumn("hub_equipment_hk", make_hk("equipment_id"))
    .withColumn("load_ts",          F.current_timestamp())
    .withColumn("record_source",    F.lit("bronze.equipment_mapping"))
    .select("hub_equipment_hk", "equipment_id", "load_ts", "record_source")
)

write_hub(hub_equipment, "hub_equipment")

# COMMAND ----------
# MAGIC %md
# MAGIC ## hub_strain
# MAGIC Business key: `strain_id` — Reb-A cell factory engineering lineage (v1 → v100).
# MAGIC Source: `bronze.strain_registry` (100 strains; tracks metabolic + enzyme + systems biology iterations).

# COMMAND ----------
print("hub_strain")

hub_strain = (
    spark.table(btbl("strain_registry"))
    .select("strain_id")
    .distinct()
    .withColumn("hub_strain_hk",  make_hk("strain_id"))
    .withColumn("load_ts",        F.current_timestamp())
    .withColumn("record_source",  F.lit("bronze.strain_registry"))
    .select("hub_strain_hk", "strain_id", "load_ts", "record_source")
)

write_hub(hub_strain, "hub_strain")

# COMMAND ----------
# MAGIC %md
# MAGIC ## hub_material
# MAGIC Business key: `material_id` — raw materials consumed in fermentation.
# MAGIC Source: `bronze.material_mapping` (7 materials: carbon sources, nitrogen sources, pH control agents, calibration).

# COMMAND ----------
print("hub_material")

hub_material = (
    spark.table(btbl("material_mapping"))
    .select("material_id")
    .distinct()
    .withColumn("hub_material_hk", make_hk("material_id"))
    .withColumn("load_ts",         F.current_timestamp())
    .withColumn("record_source",   F.lit("bronze.material_mapping"))
    .select("hub_material_hk", "material_id", "load_ts", "record_source")
)

write_hub(hub_material, "hub_material")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Hub Summary

# COMMAND ----------
print("\nHub row counts:")
for hub in ["hub_batch", "hub_equipment", "hub_strain", "hub_material"]:
    n = spark.table(stbl(hub)).count()
    print(f"  {hub:<30} {n:>6,} rows")

# COMMAND ----------
display(spark.table(stbl("hub_batch")).orderBy("batch_id").limit(5))

# COMMAND ----------
display(spark.table(stbl("hub_equipment")).orderBy("equipment_id"))

# COMMAND ----------
display(spark.table(stbl("hub_strain")).orderBy("strain_id").limit(5))

# COMMAND ----------
display(spark.table(stbl("hub_material")).orderBy("material_id"))
