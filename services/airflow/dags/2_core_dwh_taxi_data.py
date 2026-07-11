from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_PG_CONN_ID = "postgres_raw"
GP_CONN_ID = "greenplum_dwh"
SQL_BASE_PATH = Path("/opt/airflow/dags/sql/core")

default_args = {
    "owner": "yana_kel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="2_core_dwh_taxi_data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["taxi-project", "core"],
) as dag:

    def check_postgres_connection():
        """Health-check обеих БД до того, как тратить время на тяжёлую загрузку."""
        for conn_id, name in [(RAW_PG_CONN_ID, "PostgreSQL"), (GP_CONN_ID, "Greenplum")]:
            PostgresHook(postgres_conn_id=conn_id).get_conn()
            logging.info("Соединение с %s успешно установлено.", name)

    def check_postgres_src():
        """Проверка наличия и непустоты сырых данных перед загрузкой в DWH."""
        hook = PostgresHook(postgres_conn_id=RAW_PG_CONN_ID)
        for table in ["raw_taxi_trips", "raw_taxi_zones"]:
            if not hook.get_first(f"SELECT to_regclass('raw.{table}')"):
                raise ValueError(f"Таблица raw.{table} не найдена в PostgreSQL.")
            row_count = hook.get_first(f"SELECT COUNT(*) FROM raw.{table}")[0]
            logging.info("Таблица raw.%s содержит %s строк.", table, row_count)
            if row_count <= 0:
                raise ValueError(f"Таблица raw.{table} пуста!")

    def run_core_sql(sql_file: str):
        """
        Единая точка выполнения SQL-скриптов на Greenplum: DDL/мосты, загрузка
        zones и загрузка trips идут одним и тем же путём — читаем .sql-файл с
        диска и выполняем его. SQL полностью вынесен из Python, что позволяет
        версионировать и читать его отдельно от кода оркестрации.
        """
        hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
        sql_path = SQL_BASE_PATH / sql_file
        hook.run(sql_path.read_text(encoding="utf-8"), autocommit=True)
        logging.info("Скрипт %s успешно выполнен.", sql_file)

    def check_dwh_rows():
        """Пост-проверка: убеждаемся, что данные реально долетели до DWH."""
        hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
        for table in ["taxi_zones", "taxi_trips"]:
            cnt = hook.get_first(f"SELECT COUNT(*) FROM dwh.{table};")[0]
            if not cnt:
                raise ValueError(f"Таблица dwh.{table} пуста после загрузки!")
            logging.info("Таблица dwh.%s содержит %s строк.", table, cnt)

    task_check_connections = PythonOperator(
        task_id="check_database_connections", 
        python_callable=check_postgres_connection
    )

    task_check_src = PythonOperator(
        task_id="check_postgres_source", 
        python_callable=check_postgres_src
    )

    task_create_bridges = PythonOperator(
        task_id="create_pxf_bridges",
        python_callable=run_core_sql,
        op_kwargs={"sql_file": "create_ext_tables.sql"},
    )

    task_load_zones = PythonOperator(
        task_id="load_zones_to_greenplum",
        python_callable=run_core_sql,
        op_kwargs={"sql_file": "insert_zones.sql"},
    )

    task_load_trips = PythonOperator(
        task_id="load_trips_to_greenplum",
        python_callable=run_core_sql,
        op_kwargs={"sql_file": "insert_trips.sql"},
    )

    task_verify = PythonOperator(
        task_id="check_dwh_rows", 
        python_callable=check_dwh_rows
    )

    (
        task_check_connections
        >> task_check_src
        >> task_create_bridges
        >> [task_load_zones, task_load_trips]
        >> task_verify
    )
