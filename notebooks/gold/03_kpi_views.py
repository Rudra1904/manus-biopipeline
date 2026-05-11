# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold — KPI Views
# MAGIC
# MAGIC SQL views over `workspace.gold.*` fact + dimension tables.
# MAGIC Power BI connects directly to these views via the Databricks SQL warehouse.
# MAGIC
# MAGIC | View | Dashboard page | Key metric |
# MAGIC |---|---|---|
# MAGIC | `kpi_biooptimization_cycle` | BioOptimization Cycle tracker | Reb-A titer vs strain version |
# MAGIC | `kpi_equipment_oee` | Equipment OEE | Availability %, downtime, WO breakdown |
# MAGIC | `kpi_supply_risk` | Supply chain risk | Days below reorder, supply status |

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD    = "gold"
SILVER  = "silver"

def gtbl(n): return f"{CATALOG}.{GOLD}.{n}"
def stbl(n): return f"{CATALOG}.{SILVER}.{n}"

# ─────────────────────────────────────────────────────────────────────────────
# kpi_biooptimization_cycle
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## kpi_biooptimization_cycle
# MAGIC
# MAGIC Joins strain engineering metadata with actual batch performance per scale.
# MAGIC Powers the **BioOptimization Cycle tracker** dashboard page:
# MAGIC > *Line chart: Reb-A titer (g/L) per strain version — are engineering iterations improving yield?*
# MAGIC
# MAGIC `titer_vs_expected_pct` shows how actual performance compares to the design target.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD}.kpi_biooptimization_cycle AS
SELECT
    s.strain_id,
    s.engineering_modification,
    s.chassis_organism,
    s.parent_strain_id,
    s.expected_yield_g_l          AS expected_titer_g_l,
    s.improvement_over_parent_pct AS expected_improvement_pct,
    s.approved_for_scale,
    s.lead_scientist,
    t.scale_level,
    t.scale_order,
    t.batch_count,
    t.avg_reba_titer_g_l          AS actual_avg_titer_g_l,
    t.peak_reba_titer_g_l         AS actual_peak_titer_g_l,
    t.avg_yield_g_l,
    t.avg_purity_pct,
    t.pass_rate_pct,
    ROUND(
        (t.avg_reba_titer_g_l - s.expected_yield_g_l)
        / NULLIF(s.expected_yield_g_l, 0) * 100,
        2
    )                              AS titer_vs_expected_pct
FROM {CATALOG}.{GOLD}.dim_strain     s
JOIN {CATALOG}.{GOLD}.fact_scale_translation t  ON s.strain_id = t.strain_id
ORDER BY s.strain_id, t.scale_order
""")
n = spark.table(f"{CATALOG}.{GOLD}.kpi_biooptimization_cycle").count()
print(f"  [OK] kpi_biooptimization_cycle   {n:>8,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## kpi_equipment_oee
# MAGIC
# MAGIC Equipment-level operational metrics for all 12 units.
# MAGIC Powers the **Equipment OEE** dashboard page:
# MAGIC > *Gauge: availability %; bar: downtime by WO type; marker: maintenance events on batch timeline.*
# MAGIC
# MAGIC `availability_pct = total_production_hours / (production + downtime) × 100`

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD}.kpi_equipment_oee AS
SELECT
    e.equipment_id,
    e.full_name,
    e.area,
    e.scale_level,
    e.volume_l,
    -- Maintenance stats
    COALESCE(m.total_wo_count,        0)    AS total_wo_count,
    COALESCE(m.preventive_count,      0)    AS preventive_count,
    COALESCE(m.corrective_count,      0)    AS corrective_count,
    COALESCE(m.predictive_count,      0)    AS predictive_count,
    COALESCE(m.emergency_count,       0)    AS emergency_count,
    COALESCE(m.total_downtime_hours,  0.0)  AS total_downtime_hours,
    COALESCE(m.total_labor_hours,     0.0)  AS total_labor_hours,
    COALESCE(m.total_parts_cost_usd,  0.0)  AS total_parts_cost_usd,
    m.last_wo_completed_at,
    m.next_scheduled_date,
    -- Production stats (aggregated from fact_batch_run)
    COUNT(f.batch_id)                       AS total_batch_count,
    ROUND(AVG(f.avg_reba_titer_g_l), 4)    AS avg_batch_titer_g_l,
    ROUND(MAX(f.max_reba_titer_g_l), 4)    AS best_batch_titer_g_l,
    ROUND(AVG(f.batch_duration_h),   1)    AS avg_batch_duration_h,
    ROUND(SUM(f.batch_duration_h),   1)    AS total_production_hours,
    ROUND(AVG(f.avg_yield_g_l),      4)    AS avg_yield_g_l,
    SUM(CASE WHEN f.status = 'FAILED'  THEN 1 ELSE 0 END) AS failed_batch_count,
    SUM(CASE WHEN f.status = 'ABORTED' THEN 1 ELSE 0 END) AS aborted_batch_count,
    -- Availability: production time / (production + downtime)
    ROUND(
        SUM(f.batch_duration_h) /
        NULLIF(SUM(f.batch_duration_h) + COALESCE(m.total_downtime_hours, 0), 0)
        * 100,
        2
    )                                       AS availability_pct
FROM      {CATALOG}.{GOLD}.dim_equipment                e
LEFT JOIN {CATALOG}.{SILVER}.sat_equipment_maintenance  m  ON e.equipment_id = m.equipment_id
LEFT JOIN {CATALOG}.{GOLD}.fact_batch_run               f  ON e.equipment_id = f.equipment_id
GROUP BY
    e.equipment_id, e.full_name, e.area, e.scale_level, e.volume_l,
    m.total_wo_count, m.preventive_count, m.corrective_count,
    m.predictive_count, m.emergency_count,
    m.total_downtime_hours, m.total_labor_hours, m.total_parts_cost_usd,
    m.last_wo_completed_at, m.next_scheduled_date
ORDER BY total_downtime_hours DESC
""")
n = spark.table(f"{CATALOG}.{GOLD}.kpi_equipment_oee").count()
print(f"  [OK] kpi_equipment_oee           {n:>8,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## kpi_supply_risk
# MAGIC
# MAGIC Current inventory status + historical risk metrics for all 7 raw materials.
# MAGIC Powers the **Supply chain risk** dashboard page:
# MAGIC > *Bar chart: quantity on hand vs reorder point, colour-coded by `supply_status`.*
# MAGIC
# MAGIC `supply_status`: CRITICAL = currently below reorder | WARNING = <14 days supply | OK

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{GOLD}.kpi_supply_risk AS
SELECT
    m.material_id,
    m.material_name,
    m.category,
    m.unit_standard,
    -- Current snapshot state
    i.snapshot_date              AS last_snapshot_date,
    i.quantity_on_hand,
    i.reorder_point,
    i.max_stock,
    i.below_reorder_point        AS currently_below_reorder,
    i.days_of_supply,
    i.supplier_id,
    i.last_receipt_date,
    i.last_receipt_qty,
    -- Historical risk stats (across all 182 daily snapshots)
    i.days_below_reorder,
    i.total_snapshot_days,
    ROUND(
        i.days_below_reorder / NULLIF(i.total_snapshot_days, 0) * 100,
        1
    )                            AS pct_days_below_reorder,
    i.avg_qty_on_hand,
    i.avg_days_of_supply,
    -- Traffic-light supply status
    CASE
        WHEN i.below_reorder_point = TRUE     THEN 'CRITICAL'
        WHEN i.days_of_supply      < 14       THEN 'WARNING'
        ELSE                                       'OK'
    END                          AS supply_status
FROM {CATALOG}.{GOLD}.dim_material               m
JOIN {CATALOG}.{SILVER}.sat_material_inventory   i  ON m.material_id = i.material_id
ORDER BY i.days_below_reorder DESC
""")
n = spark.table(f"{CATALOG}.{GOLD}.kpi_supply_risk").count()
print(f"  [OK] kpi_supply_risk             {n:>8,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# Summary + Spot-checks
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## KPI Spot-checks

# COMMAND ----------
# BioOptimization Cycle: top strains by peak Reb-A titer at manufacturing scale
print("Top 10 manufacturing-scale strains by peak Reb-A titer:")
display(
    spark.table(f"{CATALOG}.{GOLD}.kpi_biooptimization_cycle")
    .filter(F.col("scale_level") == "manufacturing")
    .orderBy(F.desc("actual_peak_titer_g_l"))
    .select("strain_id", "engineering_modification", "expected_titer_g_l",
            "actual_avg_titer_g_l", "actual_peak_titer_g_l",
            "titer_vs_expected_pct", "pass_rate_pct", "batch_count")
    .limit(10)
)

# COMMAND ----------
# Equipment OEE: availability ranking
print("Equipment availability ranking:")
display(
    spark.table(f"{CATALOG}.{GOLD}.kpi_equipment_oee")
    .select("equipment_id", "full_name", "area", "scale_level",
            "total_batch_count", "total_production_hours",
            "total_downtime_hours", "availability_pct",
            "emergency_count", "total_parts_cost_usd")
    .orderBy(F.asc("availability_pct"))
)

# COMMAND ----------
# Supply risk: full dashboard view
print("Supply risk overview:")
display(
    spark.table(f"{CATALOG}.{GOLD}.kpi_supply_risk")
    .select("material_name", "category", "supply_status",
            "quantity_on_hand", "reorder_point", "days_of_supply",
            "days_below_reorder", "pct_days_below_reorder", "avg_days_of_supply")
    .orderBy("supply_status", F.desc("pct_days_below_reorder"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer Complete
# MAGIC
# MAGIC | Object | Type | Rows |
# MAGIC |---|---|---|
# MAGIC | dim_date | Delta table | ~365 rows |
# MAGIC | dim_batch | Delta table | 672 rows |
# MAGIC | dim_strain | Delta table | 100 rows |
# MAGIC | dim_equipment | Delta table | 12 rows |
# MAGIC | dim_material | Delta table | 7 rows |
# MAGIC | fact_batch_run | Delta table | 672 rows |
# MAGIC | fact_scale_translation | Delta table | varies (strain × scale combos) |
# MAGIC | kpi_biooptimization_cycle | SQL view | varies |
# MAGIC | kpi_equipment_oee | SQL view | 12 rows |
# MAGIC | kpi_supply_risk | SQL view | 7 rows |
# MAGIC
# MAGIC **Power BI connection**: Get Data → Azure Databricks → Server: `dbc-3da83634-2907.cloud.databricks.com`
# MAGIC → HTTP Path: `/sql/1.0/warehouses/8db7a03eee2194ef` → browse `workspace.gold.*`
