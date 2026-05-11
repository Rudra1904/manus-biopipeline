# Manus BioManufacturing Data Pipeline — Project Context

## What this project is

A portfolio data engineering project built to match the job description for **Data Engineer I at Manus Bio (Augusta, GA)**. Manus is a synthetic biology company that engineers microorganisms (yeast, bacteria) as "cell factories" to produce bioalternatives — sustainable replacements for molecules traditionally sourced from plants, animals, or fossil fuels. Their flagship product is **Yume™ M Stevia Sweetener** (Rebaudioside-A / Reb-A), produced via precision fermentation.

## Problem statement

Manus runs bioreactors at three scales (lab 2L → pilot 500L → manufacturing 50,000L). Each scale generates data in isolated silos:
- Bioreactor sensors → OT historian / DCS
- Lab QC results → LIMS
- Raw material inventory → ERP
- Equipment work orders → CMMS (SAP PM / eMaint)
- Strain engineering iterations → manual records

**No unified data model exists.** Nobody can answer: *"Why did batch #0042 underperform — was it the strain, the equipment, or the raw material lot?"* And more critically: *"Is our BioOptimization Cycle actually working — are strains improving across engineering iterations and across production scales?"*

## Solution

An end-to-end data pipeline on **Databricks + Delta Lake** using the **Medallion Architecture (Bronze → Silver → Gold)** that:
1. Ingests from all five source systems into a single platform
2. Models data using **Data Vault 2.0** at Silver layer (links batch, strain, equipment, material)
3. Exposes a **Star Schema** Gold layer for analytics
4. Powers a **Power BI dashboard** answering key operational KPIs

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Streaming | Apache Kafka | Bioreactor sensor stream (2s cadence) |
| API mock | FastAPI | Mock LIMS REST API |
| Batch source | Python scripts | CSV generator + SQLite CMMS seeder |
| Ingestion | PySpark (Databricks) | Kafka consumer, REST poller, CSV ingester, JDBC extract |
| Storage | Delta Lake on DBFS | Bronze / Silver / Gold Delta tables |
| Warehouse | Databricks SQL | Data Vault + Star Schema queries |
| Orchestration | Databricks Workflows | DAG: ingest → bronze → silver → gold → quality |
| Quality | Great Expectations | Freshness, completeness, uniqueness, range checks |
| Dashboard | Power BI Desktop | Connects natively to Databricks SQL warehouse via Azure Databricks connector |
| Version control | Git + GitHub Actions | CI: lint, unit tests, GE validation |

## Five data sources

### 1. Bioreactor sensors (Kafka streaming)
- **What**: Real-time OT sensor data from 3 bioreactors at 3 scales
- **Equipment IDs**: BR-LAB-01 (2L), BR-PILOT-01 (500L), BR-MFG-01 (50,000L)
- **Fields**: batch_id, strain_id, scale_level, timestamp, temperature_c, ph, dissolved_o2_pct, glucose_g_l, reba_titer_g_l, biomass_od600, agitation_rpm, feed_rate_ml_h, co2_evolution_rate
- **Kafka topic**: `bioreactor.sensors` partitioned by scale_level
- **Ingestion**: Kafka consumer → Delta Bronze table `bronze.bioreactor_sensors`

### 2. LIMS quality results (REST API)
- **What**: Offline QC measurements taken by lab analysts
- **Fields**: sample_id, batch_id, strain_id, sampled_at, analyst, purity_pct, yield_g_l, endotoxin_eu_ml, viability_pct, status (PASS/FAIL/PENDING)
- **Ingestion**: Watermark poller → Delta Bronze table `bronze.lims_results`

### 3. Supply chain inventory (CSV batch)
- **What**: Daily ERP export of raw material stock levels
- **Materials**: Glucose, Yeast Extract, Ammonium Sulfate, Antifoam Agent, NaOH 10%, HCl 10%
- **Fields**: material_id, name, unit, quantity_on_hand, below_reorder_point, lot_number, last_receipt_date, supplier_id
- **Ingestion**: Daily scheduled CSV pickup → Delta Bronze table `bronze.inventory_snapshots`

### 4. CMMS maintenance (SQLite / JDBC pattern)
- **What**: Equipment work orders from maintenance management system
- **Equipment**: BR-01, BR-02, CF-01 (Centrifuge), UF-01 (Ultrafiltration), CIP-01, HVAC-01
- **Fields**: wo_id, equipment_id, wo_type, priority, status, downtime_hours, labor_hours, created_at, completed_at
- **Ingestion**: Incremental SQL extract (watermark on created_at) → Delta Bronze table `bronze.work_orders`

### 5. Strain registry (SQLite / JSON — biotech-specific)
- **What**: Tracks engineering iterations of the Reb-A cell factory (unique to biotech)
- **Fields**: strain_id, parent_strain_id, target_molecule, engineering_modification, approved_for_scale, created_at
- **Example lineage**: RebA-v1 → RebA-v7 → RebA-v14 (yield improving each iteration)
- **Ingestion**: Full load on change → Delta Bronze table `bronze.strain_registry`

## Medallion Architecture

### Bronze layer
- Raw, append-only Delta tables. Never modified after write.
- Added metadata: `_ingested_at`, `_source`, `_source_file`
- Standardized: timestamps to UTC, column names to snake_case
- Schema: `bronze.*`

### Silver layer — Data Vault 2.0
Hubs (business keys), Links (relationships), Satellites (descriptive attributes over time)

**Hubs**: hub_batch, hub_equipment, hub_strain, hub_material
**Links**: lnk_batch_equipment, lnk_batch_strain, lnk_batch_material
**Satellites**: sat_batch_sensors, sat_batch_qc, sat_equipment_maintenance, sat_strain_engineering, sat_material_inventory

- Schema: `silver.*`

### Gold layer — Star Schema
**Facts**: fact_batch_run, fact_scale_translation
**Dimensions**: dim_batch, dim_strain, dim_equipment, dim_material, dim_date
**KPI views**: kpi_biooptimization_cycle, kpi_equipment_oee, kpi_supply_risk

- Schema: `gold.*`

## Dashboard pages (Power BI)

**Connection**: Power BI Desktop → Get Data → Azure → Azure Databricks → Server Hostname + HTTP Path + Personal Access Token → browse Gold schema tables directly.

1. **BioOptimization Cycle tracker** — Line chart: Reb-A titer (g/L) per strain version (v1→v14). Shows if engineering iterations are improving yield. Source: `kpi_biooptimization_cycle`
2. **Scale translation** — Clustered bar chart: same strain yield at lab vs pilot vs manufacturing. Visualizes the Valley of Death. Source: `fact_scale_translation`
3. **Batch operations monitor** — Table with conditional formatting (PASS = green, FAIL = red), slicers for date range and scale level. Source: `fact_batch_run`
4. **Equipment OEE** — Gauge visuals + bar chart per reactor, overlaid with maintenance event markers. Slicer by equipment_id. Source: `kpi_equipment_oee`
5. **Supply chain risk** — Bar chart: quantity on hand vs reorder point per material, color-coded red when below threshold. Source: `kpi_supply_risk`

**Power BI extras to add (shows enterprise maturity)**:
- Row-level security: operations staff see only their scale level (lab / pilot / manufacturing)
- Scheduled refresh connected to Databricks SQL warehouse
- Publish to Power BI Service for shareable link in portfolio

## Project folder structure

```
manus-biopipeline/
├── PROJECT_CONTEXT.md          ← this file, always share with Claude
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
│
├── sources/                    # Simulated data sources
│   ├── bioreactor/
│   │   └── producer.py         # Kafka producer — sensor stream
│   ├── lims/
│   │   └── api.py              # FastAPI mock LIMS
│   ├── supply_chain/
│   │   └── generator.py        # Daily CSV snapshot generator
│   ├── maintenance/
│   │   └── seed.py             # SQLite CMMS seeder
│   └── strain_registry/
│       └── seed.py             # Strain lineage seeder
│
├── ingestion/                  # Bronze connectors
│   ├── kafka_consumer.py       # Reads Kafka → Delta bronze.bioreactor_sensors
│   ├── lims_poller.py          # Polls LIMS API → Delta bronze.lims_results
│   ├── csv_ingester.py         # Picks up CSVs → Delta bronze.inventory_snapshots
│   ├── maintenance_ingester.py # SQL extract → Delta bronze.work_orders
│   └── strain_ingester.py      # Full load → Delta bronze.strain_registry
│
├── pipeline/
│   ├── bronze/
│   │   └── standardize.py      # Type coercion, UTC timestamps, null flags
│   ├── silver/
│   │   ├── hubs.py             # hub_batch, hub_strain, hub_equipment, hub_material
│   │   ├── links.py            # lnk_batch_equipment, lnk_batch_strain, lnk_batch_material
│   │   └── satellites.py       # All satellite tables
│   └── gold/
│       ├── facts.py            # fact_batch_run, fact_scale_translation
│       ├── dimensions.py       # All dim tables
│       └── kpi_views.py        # kpi_biooptimization_cycle, kpi_equipment_oee, kpi_supply_risk
│
├── quality/
│   ├── bronze_suite.py         # GE expectations for Bronze tables
│   └── silver_suite.py         # GE expectations for Silver tables
│
├── orchestration/
│   └── pipeline_dag.py         # Databricks Workflow definition (or Airflow DAG)
│
├── dashboard/
│   ├── manus_dashboard.pbix    # Power BI Desktop file
│   └── connection_guide.md     # How to connect Power BI to Databricks SQL
│
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
└── docs/
    ├── data_dictionary.md
    └── pipeline_diagram.md
```

## Key domain vocabulary (use these terms in code and comments)
- **Titer** (g/L): concentration of Reb-A product in the fermentation broth
- **Yield on glucose** (g/g): grams of product per gram of glucose consumed — efficiency metric
- **OD600**: optical density at 600nm — proxy for cell biomass / growth
- **Scale translation**: how well lab performance predicts pilot/manufacturing performance
- **BioOptimization Cycle**: Manus' iterative strain engineering loop (metabolic → enzyme → systems bio)
- **Valley of Death**: the gap between lab-scale proof and manufacturing-scale production
- **Precision fermentation**: using engineered microbes to produce specific molecules
- **Cell factory**: engineered microorganism used as a production host
- **cGMP**: current Good Manufacturing Practice — regulatory standard for pharma production
- **Reb-A / Rebaudioside-A**: the target steviol glycoside molecule (Yume™ sweetener)
- **Chassis host**: the base organism (yeast/bacteria) used as a starting point for engineering

## How to use this file with Claude in VS Code
At the start of any new Claude chat session, say:
"Read PROJECT_CONTEXT.md — this is the full context for what we're building. [paste content or attach file]. Now let's work on [specific component]."

Claude will have complete project context without needing the full conversation history.
