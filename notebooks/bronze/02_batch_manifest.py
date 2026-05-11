# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Batch Manifest
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/batch_manifest.csv` |
# MAGIC | **Target** | `workspace.bronze.batch_manifest` |
# MAGIC | **Load type** | Full overwrite (~672 batches across lab / pilot / manufacturing) |
# MAGIC | **Domain note** | The spine of the pipeline — links every batch to its strain, equipment, scale level, and outcome. All other Bronze tables join back here on batch_id. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/batch_manifest.csv"
TARGET_TABLE = "workspace.bronze.batch_manifest"
SOURCE_NAME  = "batch_manifest_csv"

# COMMAND ----------
# MAGIC %md ## 1 · Read raw CSV

# COMMAND ----------
schema = StructType([
    StructField("batch_id",           StringType(),  False),
    StructField("scale_level",        StringType(),  True),
    StructField("equipment_id",       StringType(),  True),
    StructField("strain_id",          StringType(),  True),
    StructField("volume_l",           IntegerType(), True),
    StructField("planned_duration_h", IntegerType(), True),
    StructField("start_time",         StringType(),  True),
    StructField("end_time",           StringType(),  True),
    StructField("status",             StringType(),  True),
    StructField("failure_reason",     StringType(),  True),
    StructField("facility",           StringType(),  True),
    StructField("product",            StringType(),  True),
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
    .withColumn("start_time", F.to_utc_timestamp(F.to_timestamp("start_time"), "UTC"))
    .withColumn("end_time",   F.to_utc_timestamp(F.to_timestamp("end_time"),   "UTC"))
    # Derive batch duration in hours (actual vs planned)
    .withColumn("actual_duration_h",
                F.round((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 3600, 2))
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
# MAGIC %md ## 4 · Verify — batch counts by scale and status

# COMMAND ----------
display(
    spark.table(TARGET_TABLE)
    .groupBy("scale_level", "status")
    .count()
    .orderBy("scale_level", "status")
)
