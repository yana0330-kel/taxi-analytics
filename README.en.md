# taxi-analytics
# NYC Taxi Data Pipeline (ETL)

*[Русская версия](README.md)*

An end-to-end analytics pipeline: from raw NYC taxi trip CSVs to 
an interactive Superset dashboard. The project demonstrates the full cycle: storage-layer design, architectural reasoning, Airflow orchestration, and building a BI data mart.

## Architecture

```
CSV (raw data)
   │
[ RAW ]  PostgreSQL — raw, unvalidated data (VARCHAR)
   │  PXF
[ CORE / DWH ]  Greenplum (MPP) — typing, cleaning
   │  PXF
[ DATA MART ]  ClickHouse — denormalized mart (One Big Table)
   │
[ BI ]  Apache Superset — dashboard for business analysis
```

All three layers are orchestrated by Apache Airflow, via three sequential DAGs:
1. `1_stage_raw_taxi_data` — load CSVs into PostgreSQL (raw)
2. `2_core_dwh_taxi_data` — transfer and type data into Greenplum (core)
3. `3_marts_clickhouse_taxi_data` — build the ClickHouse data mart

A detailed breakdown of each DAG and its tasks is in [`docs/PIPELINE.md`](docs/PIPELINE.md) (Russian; English translation in progress).

## Tech stack
* **Orchestration:** Apache Airflow
* **Raw layer:** PostgreSQL
* **Data warehouse (DWH):** Greenplum (MPP database)
* **Data marts:** ClickHouse
* **BI & dashboards:** Apache Superset
* **Infrastructure:** Docker / Docker Compose
* **Cross-database integration:** PXF (Platform Extension Framework)

## Why this architecture

Each layer solves one specific problem:
- **Raw** — ingests data as-is, with no loss if the source has issues.
- **Core/DWH** — casts data to correct types and builds a centralized, clean
  store organized by entity (trips, zones).
- **Data Mart** — a denormalized layer optimized for BI read speed rather than
  integrity.

## Dashboard

![NYC Taxi Analytics Dashboard](docs/dashboard.png)

### Key insights (January 2019, sample from NYC TLC data)

- **Total revenue** for the month — $118M across **7.58M** trips, average fare **$15.51**.
- **Average trip** — 2.82 miles and 12.92 minutes — a typical intra-city ride
  rather than an airport transfer.
- **Peak load** falls between 5–7 PM (evening rush hour).
- **Top pickup/dropoff zones**: Upper East Side South/North, Midtown Center —
  Manhattan's business and residential core dominates, as expected for NYC
  yellow taxi data.
- **Data requires cleaning before analysis**: the source dataset contains
  trips with invalid dates and zero/negative fare amounts — these are
  filtered out before reaching the mart, via quality filters in
  `extract_gp_obt_data.sql`.

## Running it locally

The project uses a `.env` file for configuration (create it based on the
variables referenced in `docker-compose.yml`).

```bash
docker compose up -d
```

If the stack was already run before and needs a restart, use `docker compose
down` + `docker compose up -d`.

To run the pipeline: in the Airflow UI (see ports below), trigger the DAGs in
order — `1_stage_raw_taxi_data` → `2_core_dwh_taxi_data` →
`3_marts_clickhouse_taxi_data`.

### Port map:
* **Apache Airflow UI:** [http://localhost:8080](http://localhost:8080)
* **Apache Superset UI:** [http://localhost:8088](http://localhost:8088)
* **PostgreSQL (Raw):** `localhost:5433` (DB: `rawdb`, schema: `raw`)
* **Greenplum (DWH):** `localhost:5434` (DB: `dwh`)
* **ClickHouse (Marts):** `localhost:8123` (DB: `dm_ch`)

## Repository structure

```
├── research/                    # EDA and ad-hoc analysis in pandas (Jupyter Notebook)
├── services/
│   ├── airflow/
│   │   ├── dags/                 # Airflow DAGs (1_stage, 2_core, 3_marts)
│   │   │   └── sql/
│   │   │       ├── raw/          # DDL / raw-layer prep
│   │   │       ├── core/         # DDL and DWH load (Greenplum)
│   │   │       └── dm/           # DDL and mart build (ClickHouse)
│   │   └── data/                 # source CSVs (not committed, see .gitignore)
│   ├── postgres/init/            # Postgres init scripts
│   ├── clickhouse/init/          # ClickHouse init scripts
│   ├── greenplum/                # Dockerfile for the custom Greenplum+PXF image
│   └── superset/                 # Superset configuration and initialization
├── docs/
│   ├── dashboard.png             # screenshot of the final dashboard
│   └── PIPELINE.md               # line-by-line breakdown of DAGs and tasks
├── docker-compose.yml
├── .gitignore
└── README.md
```
