# NYC Taxi Pipeline — DAG & Task Breakdown

[English](#english) | [Версия на русском языке](#версия-на-русском-языке)

---

## English

The pipeline consists of three DAGs executed strictly in sequence. They can be triggered manually through the Airflow UI or orchestrated via TriggerDagRunOperator if a single master DAG is introduced in the future.

## 1. 1_stage_raw_taxi_data — Load CSV Data into PostgreSQL (Raw Layer)

| Task	| Description |
|---|---|
|`check_csv_files`	| Validates that the source CSV files (`taxi_zone_lookup.csv`, `yellow_tripdata_2019_01.csv`) exist and are not empty before starting the pipeline. |
|`load_zones_to_postgres`	| Truncates `raw.raw_taxi_zones` (using `prepare_raw_taxi_zones.sql`) and loads the CSV data with `COPY ... FROM STDIN`, providing efficient bulk loading into PostgreSQL.|
|`load_trips_to_postgres`|	Performs the same operation for `raw.raw_taxi_trips` using (`prepare_raw_taxi_trips.sql`). It runs in parallel with the zones load because the two tasks are independent. |
|`check_loaded_rows` |	Post-load validation: verifies that both raw tables contain data after loading.|
**Dependencies**
`check_csv_files`
       │
       ├── `load_zones_to_postgres` ──┐
       │                             │
       └── `load_trips_to_postgres` ──┤
                                     ▼
                              `check_loaded_rows`
**Implementation Detail**

CSV column names are normalized through the COLUMN_ALIASES dictionary (for example, vendor_id → vendorid).
This is the single mapping point that needs to be updated if the NYC TLC source file format changes.

## 2. 2_core_dwh_taxi_data — Type and Load Data into Greenplum (Core Layer)
| Task	| Description |
|---|---|
|`check_database_connections` |	Performs a health check of PostgreSQL and Greenplum before the main data load. |
|`check_postgres_source` |	Verifies that the raw tables exist and contain data in PostgreSQL. |
|`create_pxf_bridges` |	Executes `create_ext_tables.sql`: recreates the target tables `dwh.taxi_zones` / `dwh.taxi_trips` and the PXF bridges (`ext.pg_taxi_zones_raw` / `ext.pg_taxi_trips_raw`) connecting Greenplum to PostgreSQL. |
|`load_zones_to_greenplum`	| Executes `insert_zones.sql` and performs explicit type conversion from VARCHAR to INTEGER through the PXF bridge. |
|`load_trips_to_greenplum` |	Executes `insert_trips.sql`, applying explicit ::CAST operations to each field and parsing timestamps with to_timestamp. Both zones and trips are loaded by executing SQL files directly; no separate Python transformation logic is used for the data load.
`check_dwh_rows`	Post-load validation: verifies that `dwh.taxi_zones` and `dwh.taxi_trips` contain data. |
**Dependencies**
`check_database_connections`
             │
             ▼
    `check_postgres_source`
             │
             ▼
      `create_pxf_bridges`
             │
       ┌─────┴─────┐
       ▼           ▼
`load_zones_     `load_trips_
to_greenplum`    to_greenplum`
       │           │
       └─────┬─────┘
             ▼
       check_dwh_rows
## 3. 3_marts_clickhouse_taxi_data — Build the ClickHouse Data Mart
| Task	| Description |
|---|---|
|`create_physical_clickhouse_table` |	Executes `create_physical_ch_table.sql` through the ClickHouse HTTP interface and recreates the physical table `dm_ch.obt_taxi_marts`. The table is partitioned by month, with data types selected to safely cover the expected value ranges. |
|`create_clickhouse_pxf_bridge` |	Executes `create_ch_obt_mart.sql` and recreates the writable PXF bridge `dm.ext_ch_obt_taxi_marts` in Greenplum. |
|`insert_into_clickhouse_via_pxf` |	Executes `extract_gp_obt_data.sql`, denormalizing the data with two LEFT JOINs to the zone reference table (pickup + drop-off) and applying data-quality filters. The period is passed through Airflow params using Jinja templating. |
|`check_mart_rows` |	Post-load validation: verifies that the data mart contains data. |
**Dependencies**
`create_physical_clickhouse_table`
             │
             ▼
`create_clickhouse_pxf_bridge`
             │
             ▼
`insert_into_clickhouse_via_pxf`
             │
             ▼
      `check_mart_rows`

**Period Parameterization:**

The DAG accepts the following parameters:
`period_start`
`period_end`

**Default values:**

period_start = 2019-01-01
period_end   = 2019-02-01

To load a different month, run the DAG using "Trigger DAG w/ config" and override these two parameters without modifying the SQL code.

## Common Principles Across All Three DAGs
- **Data integrity checks:** each DAG starts by validating its prerequisites and ends by validating the result.
- **Idempotency:** the processes can be safely re-run without unintended side effects. Target tables are fully overwritten during each run, preventing duplicate data after repeated executions.
- **Separation of logic and orchestration:** all business logic, including DDL, calculations and filters, is stored in separate .sql files under services/airflow/dags/sql/{raw,core,dm}/. The Python code in the DAGs is responsible only for orchestration — what to run, when to run it and in which order.
- **Consistent failure-handling policy:** pipeline stability is supported by standardized default_args parameters (retries=2, retry_delay=5 min, execution_timeout=30 min) designed to handle temporary infrastructure failures.

---

## Версия на русском языке

# Пайплайн: разбор DAG-ов и тасков

Три DAG-а выполняются строго последовательно (вручную через Airflow UI или через
`TriggerDagRunOperator`, если понадобится единый мастер-DAG в будущем).

## 1. `1_stage_raw_taxi_data` — загрузка CSV в PostgreSQL (raw)

| Таск | Что делает |
|---|---|
| `check_csv_files` | Проверка: убеждается, что исходные CSV (`taxi_zone_lookup.csv`, `yellow_tripdata_2019_01.csv`) физически существуют и не пустые, прежде чем запускать остальной пайплайн. |
| `load_zones_to_postgres` | Очищает `raw.raw_taxi_zones` (`prepare_raw_taxi_zones.sql`) и загружает CSV через `COPY ... FROM STDIN` — самый быстрый способ массовой загрузки в PostgreSQL. |
| `load_trips_to_postgres` | То же самое для `raw.raw_taxi_trips` (`prepare_raw_taxi_trips.sql`). Выполняется параллельно с загрузкой zones — они не зависят друг от друга. |
| `check_loaded_rows` | Пост-проверка: обе raw-таблицы должны быть непустыми после загрузки. |

Зависимости: `check_csv_files` → `[load_zones_to_postgres, load_trips_to_postgres]` → `check_loaded_rows`.

Особенность: имена колонок CSV нормализуются через словарь `COLUMN_ALIASES`
(например `vendor_id` → `vendorid`) — единственное место, которое нужно
поддерживать, если формат исходного файла NYC TLC изменится.

## 2. `2_core_dwh_taxi_data` — типизация и загрузка в Greenplum (core)

| Таск | Что делает |
|---|---|
| `check_database_connections` | Health-check PostgreSQL и Greenplum перед тяжёлой загрузкой. |
| `check_postgres_source` | Проверяет, что raw-таблицы существуют и не пусты в PostgreSQL. |
| `create_pxf_bridges` | Выполняет `create_ext_tables.sql`: пересоздаёт целевые таблицы `dwh.taxi_zones`/`dwh.taxi_trips` и PXF-мосты (`ext.pg_taxi_zones_raw`/`ext.pg_taxi_trips_raw`) к PostgreSQL. |
| `load_zones_to_greenplum` | Выполняет `insert_zones.sql` — явная типизация `VARCHAR → INTEGER` через PXF-мост. |
| `load_trips_to_greenplum` | Выполняет `insert_trips.sql` — явный `::CAST` по каждому полю + явный парсинг дат (`to_timestamp`). Оба задания (zones/trips) идут одним и тем же путём выполнения SQL-файла — никакой отдельной Python-логики для загрузки. |
| `check_dwh_rows` | Пост-проверка непустоты `dwh.taxi_zones`/`dwh.taxi_trips`. |

Зависимости: `check_database_connections` → `check_postgres_source` →
`create_pxf_bridges` → `[load_zones_to_greenplum, load_trips_to_greenplum]` →
`check_dwh_rows`.

## 3. `3_marts_clickhouse_taxi_data` — построение витрины в ClickHouse (mart)

| Таск | Что делает |
|---|---|
| `create_physical_clickhouse_table` | Выполняет `create_physical_ch_table.sql` через HTTP-интерфейс ClickHouse: пересоздаёт физическую таблицу `dm_ch.obt_taxi_marts` (партиционирована по месяцу, типы подобраны с запасом под реальный диапазон значений). |
| `create_clickhouse_pxf_bridge` | Выполняет `create_ch_obt_mart.sql` — пересоздаёт writable PXF-мост `dm.ext_ch_obt_taxi_marts` в Greenplum. |
| `insert_into_clickhouse_via_pxf` | Выполняет `extract_gp_obt_data.sql` — денормализация (2х `LEFT JOIN` к справочнику зон: посадка + высадка) и фильтры качества данных, период подставляется через Airflow `params` (Jinja). |
| `check_mart_rows` | Пост-проверка непустоты витрины. |

Зависимости: `create_physical_clickhouse_table` → `create_clickhouse_pxf_bridge`
→ `insert_into_clickhouse_via_pxf` → `check_mart_rows`.

**Параметризация периода:** DAG принимает `params` (`period_start`/`period_end`,
по умолчанию `2019-01-01`/`2019-02-01`). Чтобы загрузить другой месяц — запустить
DAG с "Trigger DAG w/ config" и переопределить эти два параметра, без правки SQL.

## Общие принципы, применённые во всех трёх DAG-ах

- **Контроль целостности**: каждый DAG начинается с проверки предпосылок и
  заканчивается проверкой результата.
- **Идемпотентность**: процессы перезапускаемы без побочных эффектов. Запись в целевые таблицы выполняется в режиме полной перезаписи (Overwrite), что исключает дублирование данных при повторных прогонах.
- **Разделение логики и оркестрации**: вся бизнес-логика (DDL, расчеты, фильтры) вынесены в отдельных `.sql`-файлах в директории `services/airflow/dags/sql/{raw,core,dm}/`, Python-код в DAG-ах отвечает только за оркестрацию (что, когда и в каком порядке выполнять).
- **Единая политика отказоустойчивости** — стабильность пайплайнов обеспечивается стандартизированными параметрами default_args (retries=2, retry_delay=5 мин, execution_timeout=30 мин), защищающими систему от кратковременных инфраструктурных сбоев.
