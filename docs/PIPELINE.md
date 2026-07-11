# Пайплайн: разбор DAG-ов и тасков

Три DAG-а выполняются строго последовательно (вручную через Airflow UI или через
`TriggerDagRunOperator`, если понадобится единый мастер-DAG в будущем).

## 1. `1_stage_raw_taxi_data` — загрузка CSV в PostgreSQL (raw)

| Таск | Что делает |
|---|---|
| `check_csv_files` | Fail-fast проверка: убеждается, что исходные CSV (`taxi_zone_lookup.csv`, `yellow_tripdata_2019_01.csv`) физически существуют и не пустые, прежде чем запускать остальной пайплайн. |
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

- **Fail-fast + verify**: каждый DAG начинается с проверки предпосылок и
  заканчивается проверкой результата.
- **Идемпотентность**: перед каждой загрузкой целевые таблицы очищаются и
  пересоздаются — повторный запуск DAG не создаёт дублей.
- **SQL вне Python**: вся бизнес-логика (DDL, INSERT, фильтры) живёт в отдельных
  `.sql`-файлах в `services/airflow/dags/sql/{raw,core,dm}/`, Python-код в DAG-ах
  отвечает только за оркестрацию (что, когда и в каком порядке выполнять).
- **`retries=2`, `retry_delay=5 мин`, `execution_timeout=30 мин`** — единые
  default_args во всех трёх DAG-ах.