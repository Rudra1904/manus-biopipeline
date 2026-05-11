# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Supply Chain Inventory Snapshots
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/supply_chain.csv` |
# MAGIC | **Target** | `workspace.bronze.inventory_snapshots` |
# MAGIC | **Load type** | Full overwrite (~1,274 daily snapshots × 7 materials = 6 months) |
# MAGIC | **Domain note** | Daily ERP export of raw material stock levels. Glucose is consumed fastest (~15 kg/day). below_reorder_point = True signals supply risk for the dashboard. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/supply_chain.csv"
TARGET_TABLE = "workspace.bronze.inventory_snapshots"
SOURCE_NAME  = "erp_supply_chain_csv"

# COMMAND ----------
# MAGIC %md ## 1 · Read raw CSV

# COMMAND ----------
schema = StructType([
    StructField("snapshot_date",       StringType(),  True),
    StructField("material_id",         StringType(),  False),
    StructField("material_name",       StringType(),  True),
    StructField("unit",                StringType(),  True),
    StructField("quantity_on_hand",    DoubleType(),  True),
    StructField("reorder_point",       DoubleType(),  True),
    StructField("max_stock",           DoubleType(),  True),
    StructField("below_reorder_point", BooleanType(), True),
    StructField("days_of_supply",      DoubleType(),  True),
    StructField("lot_number",          StringType(),  True),
    StructField("supplier_id",         StringType(),  True),
    StructField("last_receipt_qty",    DoubleType(),  True),
    StructField("last_receipt_date",   StringType(),  True),
    StructField("snapshot_ts",         StringType(),  True),
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
    # Parse date and timestamp columns
    .withColumn("snapshot_date",     F.to_date("snapshot_date",   "yyyy-MM-dd"))
    .withColumn("last_receipt_date", F.to_date("last_receipt_date","yyyy-MM-dd"))
    .withColumn("snapshot_ts",       F.to_utc_timestamp(F.to_timestamp("snapshot_ts"), "UTC"))
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
# MAGIC %md ## 4 · Verify — supply risk days by material

# COMMAND ----------
display(
    spark.table(TARGET_TABLE)
    .groupBy("material_name", "unit")
    .agg(
        F.sum(F.col("below_reorder_point").cast("int")).alias("days_below_reorder"),
        F.round(F.avg("quantity_on_hand"), 1).alias("avg_qty_on_hand"),
        F.round(F.avg("days_of_supply"),   1).alias("avg_days_of_supply")
    )
    .orderBy(F.desc("days_below_reorder"))
)
