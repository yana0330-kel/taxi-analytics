from __future__ import annotations

import logging
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

GP_CONN_ID = "greenplum_dwh"
CH_CONN_ID = "dm_clickhouse"
SQL_BASE_PATH = "/opt/airflow/dags/sql/dm"

default_args = {"owner": "yana_kel", "retries": 1}

def read_sql_file(file_name: str) -> str:
    with open(f"{SQL_BASE_PATH}/{file_name}", "r", encoding="utf-8") as f:
        return f.read()

def create_physical_ch_table():
    conn = BaseHook.get_connection(CH_CONN_ID)
    url = f"http://{conn.host}:{conn.port}/"
    sql_query = read_sql_file("create_physical_ch_table.sql")
    headers = {'X-ClickHouse-User': conn.login, 'X-ClickHouse-Key': conn.password} if conn.login else {}

    for statement in sql_query.split(";"):
        if statement.strip():
            requests.post(url, data=statement.strip().encode('utf-8'), headers=headers)
    logging.info("Физическая таблица в ClickHouse готова.")

def run_marts_sql(sql_file: str):
    hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
    hook.run(read_sql_file(sql_file), autocommit=True)
    logging.info("Скрипт %s выполнен.", sql_file)


with DAG(
    dag_id="3_marts_clickhouse_taxi_data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["taxi-project", "marts", "clickhouse", "pxf"],
) as dag:

    task_create_ch = PythonOperator(
        task_id="create_physical_clickhouse_table",
        python_callable=create_physical_ch_table,
    )

    task_create_bridge = PythonOperator(
        task_id="create_clickhouse_pxf_bridge",
        python_callable=run_marts_sql,
        op_kwargs={"sql_file": "create_ch_obt_mart.sql"}, 
    )

    task_insert_via_pxf = PythonOperator(
        task_id="insert_into_clickhouse_via_pxf",
        python_callable=run_marts_sql,
        op_kwargs={"sql_file": "extract_gp_obt_data.sql"},
    )

    task_create_ch >> task_create_bridge >> task_insert_via_pxf