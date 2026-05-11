# Data Source Survey — Manus BioManufacturing Pipeline

## Overview

Catalogs all six data sources ingested into the Bronze layer of the Manus BioManufacturing Data Pipeline. Silver reads exclusively from the `*_clean` views produced by `07_data_cleaning.py`.

## Source Inventory

| Source | System Type | Owner | Format | Ingestion Type | Frequency | Retention | Known Quality Issues |
|---|---|---|---|---|---|---|---|
| **Bioreactor Sensors** (`bioreactor_sensors`) | SCADA / IoT historian | Process Engineering | Parquet, partitioned by `scale_level` (15 files, ~1.4 GB) | Full batch overwrite | Daily batch export; 2-second cadence within each batch run | 2 years | ~0.045 % temperature outliers (outside 30–42 °C); ~0.045 % negative Reb-A titer readings during bioreactor startup phase |
| **Batch Manifest** (`batch_manifest`) | MES (Manufacturing Execution System) | Manufacturing | CSV | Full overwrite | Per batch close event | 7 years (cGMP requirement) | None identified in current dataset |
| **LIMS Results** (`lims_results`) | LIMS API export | QC Lab | CSV | Full overwrite | Per batch — 3 samples per batch (early IPC, mid IPC, end-of-batch) | 7 years (cGMP requirement) | 2 endotoxin readings > 5.0 EU/mL (pipeline outlier threshold); cGMP product-release limit is 1.0 EU/mL, flagged separately via `endotoxin_limit_breach` column |
| **Inventory Snapshots** (`inventory_snapshots`) | ERP daily export | Supply Chain | CSV | Daily full snapshot | Daily (7 materials × ~182 days = 1,274 rows) | 1 year | None identified in current dataset |
| **Work Orders / CMMS** (`work_orders`) | CMMS (Computerised Maintenance Management System) | Maintenance Engineering | CSV | Full overwrite | Continuous — as work orders are created and closed | 5 years | None identified in current dataset |
| **Strain Registry** (`strain_registry`) | Internal strain database | R&D / Strain Science | CSV | Full overwrite | On new strain registration | Indefinite (IP asset) | None identified in current dataset |

## DQ Flag Reference

| Flag | Table | Meaning | Action |
|---|---|---|---|
| `NULL_CRITICAL` | bioreactor_sensors | `ph` or `reba_titer_g_l` is NULL | Row excluded from clean view |
| `OUTLIER_TEMP` | bioreactor_sensors | `temperature_c` outside 30–42 °C | Row excluded from clean view |
| `OUTLIER_TITER` | bioreactor_sensors | `reba_titer_g_l` < 0 | Row excluded from clean view |
| `OUTLIER_PH` | bioreactor_sensors | `ph` outside 6.0–8.5 | Row excluded from clean view |
| `OUTLIER_DO2` | bioreactor_sensors | `dissolved_o2_pct` outside 0–100 | Row excluded from clean view |
| `OUTLIER_PURITY` | lims_results | `purity_pct` outside 0–100 | Row excluded from clean view |
| `OUTLIER_ENDOTOXIN` | lims_results | `endotoxin_eu_ml` > 5.0 EU/mL | Row excluded from clean view |
| `ORPHAN_BATCH_ID` | lims_results | `batch_id` not found in `batch_manifest` | Row excluded from clean view |
| `RULE_FAIL_PASS_MISMATCH` | lims_results | Batch status FAILED/ABORTED but LIMS result is PASS | Row excluded from clean view |
| `UNKNOWN_BATCH` | bioreactor_sensors | `batch_id` not found in `batch_manifest` | Row excluded from clean view |
| `RULE_INVALID_DATES` | work_orders | `completed_at` < `created_at` | Row excluded from clean view |

## Clean Views (Silver entry points)

| View | Source | Purpose |
|---|---|---|
| `workspace.bronze.bioreactor_sensors_clean` | `bioreactor_sensors` WHERE `_dq_flag IS NULL` | Primary sensor signal for Silver hubs and satellites |
| `workspace.bronze.lims_results_clean` | `lims_results` WHERE `_dq_flag IS NULL` | QC results for Silver — cGMP compliance tracking |
| `workspace.bronze.work_orders_clean` | `work_orders` WHERE `_dq_flag IS NULL` | Maintenance records for equipment downtime analysis |

## Reference Tables

| Table | Rows | Purpose |
|---|---|---|
| `workspace.bronze.equipment_mapping` | 12 | Canonical equipment IDs → full name, scale, area, vessel volume (L) |
| `workspace.bronze.material_mapping` | 7 | Raw material IDs → standard unit + category for supply chain joins |

## Audit Trail

Every run of `07_data_cleaning.py` overwrites `workspace.bronze.dq_report` with one row per check:

```
table_name | check_name | records_checked | records_flagged | flagged_pct | run_timestamp
```

Use this table to track data quality trends across pipeline runs.
