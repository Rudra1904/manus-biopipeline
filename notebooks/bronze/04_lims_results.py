# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — LIMS Quality Results
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/lims_results.csv` |
# MAGIC | **Target** | `workspace.bronze.lims_results` |
# MAGIC | **Load type** | Full overwrite (~2,016 QC samples across all batches) |
# MAGIC | **Domain note** | 3 samples per batch (early IPC, mid IPC, end-of-batch). Tracks purity %, yield g/L, endotoxin EU/mL, viability %. Critical for cGMP compliance — endotoxin must be <1.0 EU/mL for release. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/lims_results.csv"
TARGET_TABLE = "workspace.bronze.lims_results"
SOURCE_NAME  = "lims_api_csv"

# COMMAND ----------
# MAGIC %md ## 1 · Read raw CSV

# COMMAND ----------
schema = StructType([
    StructField("sample_id",        StringType(), False),
    StructField("batch_id",         StringType(), True),
    StructField("strain_id",        StringType(), True),
    StructField("equipment_id",     StringType(), True),
    StructField("scale_level",      StringType(), True),
    StructField("sample_type",      StringType(), True),
    StructField("sampled_at",       StringType(), True),
    StructField("analyst",          StringType(), True),
    StructField("purity_pct",       DoubleType(), True),
    StructField("yield_g_l",        DoubleType(), True),
    StructField("viability_pct",    DoubleType(), True),
    StructField("endotoxin_eu_ml",  DoubleType(), True),
    StructField("od600_at_sample",  DoubleType(), True),
    StructField("ph_at_sample",     DoubleType(), True),
    StructField("status",           StringType(), True),
    StructField("reviewed_by",      StringType(), True),
    StructField("reviewed_at",      StringType(), True),
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
    # Parse ISO timestamps → UTC
    .withColumn("sampled_at",   F.to_utc_timestamp(F.to_timestamp("sampled_at"),   "UTC"))
    .withColumn("reviewed_at",  F.to_utc_timestamp(F.to_timestamp("reviewed_at"),  "UTC"))
    # Flag cGMP endotoxin limit breach (>1.0 EU/mL = potential release failure)
    .withColumn("endotoxin_limit_breach", F.col("endotoxin_eu_ml") > 1.0)
    # Bronze envelope
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source",      F.lit(SOURCE_NAME))
    .withColumn("_source_file", F.lit(SOURCE_PATH))
)

# COMMAND ----------
# MAGIC %md ## 3 · Write to Delta

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
# MAGIC %md ## 4 · Verify — QC status breakdown and avg end-of-batch yield

# COMMAND ----------
display(
    spark.table(TARGET_TABLE)
    .groupBy("scale_level", "status")
    .agg(
        F.count("*").alias("sample_count"),
        F.round(F.avg("purity_pct"),      2).alias("avg_purity_pct"),
        F.round(F.avg("yield_g_l"),       3).alias("avg_yield_g_l"),
        F.round(F.avg("endotoxin_eu_ml"), 3).alias("avg_endotoxin_eu_ml")
    )
    .orderBy("scale_level", "status")
)
