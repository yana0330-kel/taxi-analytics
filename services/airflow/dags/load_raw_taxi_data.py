from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "postgres_raw"
DATA_DIR = Path("/opt/airflow/data")
ZONES_CSV = DATA_DIR / "taxi_zone_lookup.csv"
TRIPS_CSV = DATA_DIR / "yellow_tripdata_2019_01.csv"

default_args = {"owner": "yana_kel", "retries": 1}

with DAG(
    dag_id="load_new_york_taxi_data",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="/opt/airflow/dags/sql",
    tags=["taxi-project", "raw"],
) as dag:

    def check_csv_files():
        for csv_path in [ZONES_CSV, TRIPS_CSV]:
            if not csv_path.exists() or csv_path.stat().st_size == 0:
                raise ValueError(f"Ошибка с файлом: {csv_path}")
            logging.info(f"Файл готов: {csv_path.name}")

    def load_csv_to_postgres(file_path: Path, table_name: str, sql_file: str):
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        
        # 1. Очищаем таблицу перед новой заливкой
        logging.info(f"Очистка {table_name} через {sql_file}")
        sql_query = Path(f"/opt/airflow/dags/sql/{sql_file}").read_text(encoding="utf-8")
        hook.run(sql_query, autocommit=True)

        logging.info(f"Ультра-загрузка файла {file_path.name} через COPY...")
        
        # 2. Открываем файл на чтение и стримим его напрямую в Postgres через сокет
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Читаем первую строчку файла, чтобы узнать точные имена колонок в CSV
                    header_line = f.readline().strip()
                    # Приводим их к нижнему регистру, как в нашей таблице Postgres
                    csv_columns = header_line.lower()
                    csv_columns = csv_columns.replace('location_id', 'locationid').replace('vendor_id', 'vendorid').replace('ratecode_id', 'ratecodeid').replace('pulocation_id', 'pulocationid').replace('dolocation_id', 'dolocationid') 
                    
                    # Возвращаем указатель в начало файла, чтобы COPY прочитал его целиком
                    f.seek(0)
                    
                    # Указываем базе данных, какие именно колонки мы берем из CSV (пропуская dt)
                    sql_copy = f"COPY raw.{table_name} ({csv_columns}) FROM STDIN WITH DELIMITER ',' CSV HEADER NULL AS '';"
                    cur.copy_expert(sql_copy, f)
                
        logging.info(f"Файл {file_path.name} успешно загружен в таблицу raw.{table_name}!")

    def check_loaded_rows():
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        for table in ["raw_taxi_zones", "raw_taxi_trips"]:
            cnt = hook.get_first(f"SELECT count(*) FROM raw.{table};")[0]
            if not cnt:
                raise ValueError(f"Таблица raw.{table} пуста!")
            logging.info(f"Таблица raw.{table} содержит {cnt} строк.")

    # Операторы
    task_check = PythonOperator(task_id="check_csv_files", python_callable=check_csv_files)
    
    task_zones = PythonOperator(
        task_id="load_zones_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_path': ZONES_CSV, 'table_name': 'raw_taxi_zones', 'sql_file': 'truncate_zones.sql'}
    )

    task_trips = PythonOperator(
        task_id="load_trips_to_postgres",
        python_callable=load_csv_to_postgres,
        op_kwargs={'file_path': TRIPS_CSV, 'table_name': 'raw_taxi_trips', 'sql_file': 'truncate_trips.sql'}
    )

    task_verify = PythonOperator(task_id="check_loaded_rows", python_callable=check_loaded_rows)

    task_check >> [task_zones, task_trips] >> task_verify