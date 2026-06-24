from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

PG_CONN_ID = "postgres_raw"
DATA_DIR = Path("/opt/airflow/data")
ZONES_CSV = DATA_DIR / "taxi_zone_lookup.csv"
TRIPS_CSV = DATA_DIR / "yellow_tripdata_2019_01.csv"

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
        
        logging.info(f"Очистка {table_name} через {sql_file}")
        sql_query = Path(f"/opt/airflow/dags/sql/{sql_file}").read_text(encoding="utf-8")
        hook.run(sql_query, autocommit=True)

        engine = create_engine(hook.get_uri())
        logging.info(f"Загрузка {file_path.name}...")

        # Маппинг колонок для поездок (чтобы не писать огромный список руками)
        columns_map = {
            'vendorid': 'vendor_id', 'ratecodeid': 'ratecode_id',
            'pulocationid': 'pulocation_id', 'dolocationid': 'dolocation_id'
        }

        for chunk in pd.read_csv(file_path, chunksize=100_000):
            chunk.columns = [c.lower() for c in chunk.columns]
            if table_name == 'taxi_trips':
                chunk = chunk.rename(columns=columns_map)
            elif table_name == 'raw_taxi_zones':
                chunk.columns = ['locationid', 'borough', 'zone', 'service_zone']

            chunk.to_sql(name=table_name, con=engine, schema="raw", if_exists="append", index=False)

    def check_loaded_rows():
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        for table in ["raw_taxi_zones", "taxi_trips"]:
            cnt = hook.get_first(f"SELECT count(*) FROM raw.{table};")[0]
            if not cnt:
                raise ValueError(f"Таблица raw.{table} пуста!")
            logging.info(f"Таблица raw.{table}: {cnt} строк.")

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
        op_kwargs={'file_path': TRIPS_CSV, 'table_name': 'taxi_trips', 'sql_file': 'truncate_trips.sql'}
    )

    task_verify = PythonOperator(task_id="check_loaded_rows", python_callable=check_loaded_rows)

    task_check >> [task_zones, task_trips] >> task_verify