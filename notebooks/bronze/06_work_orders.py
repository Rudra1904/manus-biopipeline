# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — CMMS Maintenance Work Orders
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/maintenance.csv` |
# MAGIC | **Target** | `workspace.bronze.work_orders` |
# MAGIC | **Load type** | Full overwrite (~543 work orders across 12 equipment units) |
# MAGIC | **Domain note** | Equipment maintenance records (Preventive / Corrective / Predictive / Emergency). Downtime hours link to batch failure root-cause analysis. Manufacturing-scale equipment (BR-MFG-01) has weekly PM schedule per cGMP requirements. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/maintenance.csv"
TARGET_TABLE = "workspace.bronze.work_orders"
SOURCE_NAME  = "cmms_maintenance_csv"

# COMMAND ----------
# MAGIC %md ## 1 · Read raw CSV

# COMMAND ----------
schema = StructType([
    StructField("wo_id",           StringType(), False),
    StructField("equipment_id",    StringType(), True),
    StructField("equipment_name",  StringType(), True),
    StructField("area",            StringType(), True),
    StructField("scale_level",     StringType(), True),
    StructField("wo_type",         StringType(), True),
    StructField("priority",        StringType(), True),
    StructField("status",          StringType(), True),
    StructField("description",     StringType(), True),
    StructField("technician",      StringType(), True),
    StructField("created_at",      StringType(), True),
    StructField("scheduled_date",  StringType(), True),
    StructField("completed_at",    StringType(), True),
    StructField("downtime_hours",  DoubleType(), True),
    StructField("labor_hours",     DoubleType(), True),
    StructField("parts_cost_usd",  DoubleType(), True),
    StructField("failure_code",    StringType(), True),
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
    .withColumn("created_at",     F.to_utc_timestamp(F.to_timestamp("created_at"),    "UTC"))
    .withColumn("scheduled_date", F.to_utc_timestamp(F.to_timestamp("scheduled_date"),"UTC"))
    .withColumn("completed_at",   F.to_utc_timestamp(F.to_timestamp("completed_at"),  "UTC"))
    # Derive work order age in days (for backlog analysis)
    .withColumn("wo_age_days",
                F.when(F.col("completed_at").isNotNull(),
                       F.datediff("completed_at", "created_at"))
                .otherwise(F.datediff(F.current_date(), F.to_date("created_at"))))
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
# MAGIC %md ## 4 · Verify — downtime by equipment and work order type

# COMMAND ----------
display(
    spark.table(TARGET_TABLE)
    .groupBy("equipment_id", "wo_type")
    .agg(
        F.count("*").alias("wo_count"),
        F.round(F.sum("downtime_hours"),  1).alias("total_downtime_h"),
        F.round(F.sum("parts_cost_usd"),  2).alias("total_parts_cost_usd")
    )
    .orderBy("equipment_id", "wo_type")
)
