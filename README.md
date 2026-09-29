# NYC Taxi Data Analytics

*[Русская версия](README.en.md)*

An end-to-end analytics project based on NYC taxi trip data, covering the full path from raw data to an interactive BI dashboard.

The project combines data analysis, SQL, DWH, ETL/ELT orchestration and BI to explore revenue, trip volume, demand patterns and geographic trends.

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

The three layers are orchestrated by Apache Airflow, via three sequential DAGs:
1. `1_stage_raw_taxi_data` — loads CSV data into PostgreSQL (raw layer)
2. `2_core_dwh_taxi_data` — transfers and transforms data into Greenplum (core layer)
3. `3_marts_clickhouse_taxi_data` — builds the analytical data mart in ClickHouse

A detailed breakdown of each DAG and its tasks is in [`docs/PIPELINE.md`](docs/PIPELINE.md)

## Tech stack
* **Analytics & SQL:** SQL, PostgreSQL
* **Data Warehouse:** Greenplum (MPP)
* **Data Mart:** ClickHouse
* **Orchestration:** Apache Airflow
* **BI & Visualization:** Apache Superset
* **Infrastructure:** Docker / Docker Compose
* **Cross-database integration:** PXF
* **Analysis:** Python, Pandas, Jupyter Notebook

## Why this architecture

Each layer solves one specific problem:
- **Raw** — stores source data with minimal transformation.
- **Core/DWH** — cleans and types data and organizes analytical entities such as trips and zones.
- **Data Mart** — provides a denormalized structure optimized for BI queries and analysis.

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
