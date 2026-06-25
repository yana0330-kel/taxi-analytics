from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_PG_CONN_ID = "postgres_raw"
GP_CONN_ID = "greenplum_dwh"

default_args = {"owner": "yana_kel", "retries": 1}

with DAG(
    dag_id="extract_core_taxi_data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["taxi-project", "core"],
) as dag:

    def check_postgres_connection():
        """1. Проверка доступности баз данных."""
        for conn_id, name in [(RAW_PG_CONN_ID, "PostgreSQL"), (GP_CONN_ID, "Greenplum")]:
            PostgresHook(postgres_conn_id=conn_id).get_conn()
            logging.info(f"Соединение с {name} успешно установлено.")

    def check_postgres_scr():
        """2. Проверка наличия сырых данных в источнике."""
        hook = PostgresHook(postgres_conn_id=RAW_PG_CONN_ID)
        for table in ["raw_taxi_trips", "raw_taxi_zones"]:
            if not hook.get_first(f"SELECT to_regclass('raw.{table}')"):
                raise ValueError(f"Таблица raw.{table} не найдена в PostgreSQL.")
            
            row_count = hook.get_first(f"SELECT COUNT(*) FROM raw.{table}")
            logging.info(f"Таблица raw.{table} содержит {row_count[0]} строк.")
            if row_count[0] <= 0:
                raise ValueError(f"Таблица raw.{table} пуста!")

    def run_core_sql(sql_file: str):
        """3. Универсальный запуск SQL-файлов."""
        hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
        sql_path = Path(f"/opt/airflow/dags/sql/core/{sql_file}")
        hook.run(sql_path.read_text(encoding="utf-8"), autocommit=True)
        logging.info(f"Скрипт {sql_file} успешно выполнен.")

    def python_load_trips_to_greenplum():
        """4. Высокоскоростной стриминг COPY с автоматическим чтением колонок из SQL-файла."""
        pg_hook = PostgresHook(postgres_conn_id=RAW_PG_CONN_ID)
        gp_hook = PostgresHook(postgres_conn_id=GP_CONN_ID)

        gp_hook.run("TRUNCATE TABLE dwh.taxi_trips;", autocommit=True)

       
        sql_text = Path("/opt/airflow/dags/sql/core/insert_trips.sql").read_text(encoding="utf-8")
        columns = re.findall(r"\b([a-z_0-9]+)\s*(?:,|$)", sql_text.split("SELECT")[0].split("(")[1])
        cols_str = ", ".join(columns)

        with pg_hook.get_conn() as pg_conn, gp_hook.get_conn() as gp_conn:
            with pg_conn.cursor() as pg_cur, gp_conn.cursor() as gp_cur:
                buffer = io.StringIO()

                
                pg_cur.copy_expert(f"COPY raw.raw_taxi_trips ({cols_str}) TO STDOUT WITH DELIMITER '\t';", buffer)
                buffer.seek(0)
                gp_cur.copy_expert(f"COPY dwh.taxi_trips ({cols_str}) FROM STDIN WITH DELIMITER '\t';", buffer)
                gp_conn.commit()

        logging.info(f"Ультра-загрузка {len(columns)} колонок успешно завершена!")

    
    task_check_connections = PythonOperator(
        task_id="check_database_connections", 
        python_callable=check_postgres_connection
        )
    
    task_check_src = PythonOperator(
        task_id="check_postgres_source", 
        python_callable=check_postgres_scr
        )
    
    task_create_bridges = PythonOperator(
        task_id="create_pxf_bridges", 
        python_callable=run_core_sql, 
        op_kwargs={'sql_file': 'create_ext_tables.sql'}
        )
    
    task_load_zones = PythonOperator(
        task_id="load_zones_to_greenplum",
        python_callable=run_core_sql, 
        op_kwargs={'sql_file': 'insert_zones.sql'}
        )
    
    task_load_trips = PythonOperator(
        task_id="load_trips_to_greenplum", 
        python_callable=python_load_trips_to_greenplum
        )

    task_check_connections >> task_check_src >> task_create_bridges >> [task_load_zones, task_load_trips]