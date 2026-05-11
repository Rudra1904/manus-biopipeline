# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Bioreactor Sensors
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/workspace/bronze/raw_uploads/sensors/` (Parquet, partitioned by scale) |
# MAGIC | **Target** | `workspace.bronze.bioreactor_sensors` (Delta, partitioned by `scale_level`) |
# MAGIC | **Load type** | Full overwrite — ~88.9 M rows across 15 Parquet files |
# MAGIC | **Domain note** | 2-second cadence sensor readings: temperature, pH, DO, glucose, Reb-A titer, OD600, agitation, CO2. Primary signal for batch performance and the BioOptimization Cycle tracker. |

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, LongType
)

SOURCE_PATH  = "/Volumes/workspace/bronze/raw_uploads/sensors/"
TARGET_TABLE = "workspace.bronze.bioreactor_sensors"
SOURCE_NAME  = "bioreactor_sensors_parquet"

# COMMAND ----------
# MAGIC %md ## 1 · Read Parquet — explicit schema

# COMMAND ----------
schema = StructType([
    StructField("batch_id",           StringType(),    False),
    StructField("strain_id",          StringType(),    True),
    StructField("equipment_id",       StringType(),    True),
    StructField("scale_level",        StringType(),    True),
    StructField("timestamp",          TimestampType(), True),
    StructField("temperature_c",      DoubleType(),    True),
    StructField("ph",                 DoubleType(),    True),
    StructField("dissolved_o2_pct",   DoubleType(),    True),
    StructField("glucose_g_l",        DoubleType(),    True),
    StructField("biomass_od600",      DoubleType(),    True),
    StructField("agitation_rpm",      LongType(),      True),
    StructField("co2_evolution_rate", DoubleType(),    True),
    StructField("reba_titer_g_l",     DoubleType(),    True),
])

df_raw = (
    spark.read
    .schema(schema)
    .parquet(SOURCE_PATH)
    # Directory partition 'scale=lab' produces a 'scale' column — drop it; scale_level is inside
    .drop("scale")
)

print(f"Schema OK — {len(df_raw.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 2 · Standardise + add Bronze metadata

# COMMAND ----------
df_bronze = (
    df_raw
    # Ensure timestamp is UTC (already set by datagen, enforce it)
    .withColumn("timestamp", F.to_utc_timestamp("timestamp", "UTC"))
    # Derive date column for efficient partition pruning in Silver/Gold queries
    .withColumn("sensor_date", F.to_date("timestamp"))
    # Bronze envelope
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source",      F.lit(SOURCE_NAME))
    .withColumn("_source_file", F.lit(SOURCE_PATH))
)

# COMMAND ----------
# MAGIC %md ## 3 · Write to Delta — partitioned by scale_level for query performance

# COMMAND ----------
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("scale_level")
    .saveAsTable(TARGET_TABLE)
)

count = spark.table(TARGET_TABLE).count()
print(f"[OK] {TARGET_TABLE}: {count:,} rows")

# COMMAND ----------
# MAGIC %md ## 4 · Verify — row counts and avg Reb-A titer by scale

# COMMAND ----------
display(
    spark.table(TARGET_TABLE)
    .groupBy("scale_level")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("reba_titer_g_l"), 4).alias("avg_reba_titer_g_l"),
        F.round(F.avg("temperature_c"),  2).alias("avg_temp_c"),
        F.round(F.avg("ph"),             3).alias("avg_ph"),
        F.countDistinct("batch_id").alias("distinct_batches")
    )
    .orderBy("scale_level")
)
