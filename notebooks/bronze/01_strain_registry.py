# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Strain Registry
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/strain_registry.csv` |
# MAGIC | **Target** | `workspace.bronze.strain_registry` |
# MAGIC | **Load type** | Full overwrite (100 rows, changes infrequently) |
# MAGIC | **Domain note** | Tracks RebA cell factory engineering lineage (v1 → v100). Every batch references a strain_id from this table. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/strain_registry.csv"
TARGET_TABLE = "workspace.bronze.strain_registry"
SOURCE_NAME  = "strain_registry_csv"

# COMMAND ----------
# MAGIC %md ## 1 · Read raw CSV with explicit schema

# COMMAND ----------
schema = StructType([
    StructField("strain_id",                   StringType(), False),
    StructField("parent_strain_id",            StringType(), True),
    StructField("target_molecule",             StringType(), True),
    StructField("chassis_organism",            StringType(), True),
    StructField("engineering_modification",    StringType(), True),
    StructField("expected_yield_g_l",          DoubleType(), True),
    StructField("improvement_over_parent_pct", DoubleType(), True),
    StructField("approved_for_scale",          StringType(), True),
    StructField("lead_scientist",              StringType(), True),
    StructField("created_at",                  StringType(), True),
    StructField("notes",                       StringType(), True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(SOURCE_PATH)
)

print(f"Raw rows read: {df_raw.count()}")

# COMMAND ----------
# MAGIC %md ## 2 · Standardise + add Bronze metadata

# COMMAND ----------
df_bronze = (
    df_raw
    # Parse ISO timestamp → UTC
    .withColumn("created_at", F.to_utc_timestamp(F.to_timestamp("created_at"), "UTC"))
    # Bronze envelope columns
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source",      F.lit(SOURCE_NAME))
    .withColumn("_source_file", F.lit(SOURCE_PATH))
)

# COMMAND ----------
# MAGIC %md ## 3 · Write to Delta (overwrite — full reload)

# COMMAND ----------
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"[OK] {TARGET_TABLE}: {count:,} rows")

# COMMAND ----------
# MAGIC %md ## 4 · Verify

# COMMAND ----------
display(spark.table(TARGET_TABLE).orderBy("strain_id").limit(10))
