from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from jinja2 import Template

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

GP_CONN_ID = "greenplum_dwh"
CH_CONN_ID = "dm_clickhouse"
SQL_BASE_PATH = Path("/opt/airflow/dags/sql/dm")

default_args = {
    "owner": "yana_kel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# Параметризованный период загрузки — передаётся в SQL-шаблон через Jinja.
# Чтобы загрузить другой месяц, достаточно переопределить params при запуске
# DAG (Trigger DAG w/ config), без правки SQL-файлов.
DEFAULT_PARAMS = {
    "period_start": "2019-01-01",
    "period_end": "2019-02-01",
}


def read_sql_file(file_name: str) -> str:
    return (SQL_BASE_PATH / file_name).read_text(encoding="utf-8")


def create_physical_ch_table():
    """
    DDL в ClickHouse через HTTP-интерфейс.
    NB: для продакшена лучше использовать официальный клиент (clickhouse-connect /
    clickhouse-driver) вместо ручного requests.post + split(';') — это устойчивее
    к SQL с комментариями/строковыми литералами, содержащими ';'. Здесь оставлено
    просто, т.к. DDL-файл маленький и статичный.
    """
    conn = BaseHook.get_connection(CH_CONN_ID)
    url = f"http://{conn.host}:{conn.port}/"
    headers = {"X-ClickHouse-User": conn.login, "X-ClickHouse-Key": conn.password} if conn.login else {}

    sql_query = read_sql_file("create_physical_ch_table.sql")
    statements = [s.strip() for s in sql_query.split(";") if s.strip()]
    for statement in statements:
        resp = requests.post(url, data=statement.encode("utf-8"), headers=headers)
        resp.raise_for_status()  # явная проверка ответа ClickHouse на каждый DDL-стейтмент
    logging.info("Физическая таблица в ClickHouse готова (%s стейтментов).", len(statements))


def run_marts_sql(sql_file: str, **context):
    """Выполняет SQL-файл на Greenplum, подставляя params через Jinja (даты периода)."""
    hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
    raw_sql = read_sql_file(sql_file)
    params = context["params"]
    rendered_sql = Template(raw_sql).render(params=params)
    hook.run(rendered_sql, autocommit=True)
    logging.info("Скрипт %s выполнен для периода %s — %s.", sql_file, params["period_start"], params["period_end"])


def check_mart_rows(**context):
    """Пост-проверка: убеждаемся, что данные реально долетели до витрины."""
    conn = BaseHook.get_connection(CH_CONN_ID)
    url = f"http://{conn.host}:{conn.port}/"
    headers = {"X-ClickHouse-User": conn.login, "X-ClickHouse-Key": conn.password} if conn.login else {}
    resp = requests.post(url, data=b"SELECT count(*) FROM dm_ch.obt_taxi_marts", headers=headers)
    resp.raise_for_status()
    count = int(resp.text.strip())
    if count == 0:
        raise ValueError("Витрина dm_ch.obt_taxi_marts пуста после загрузки!")
    logging.info("Витрина dm_ch.obt_taxi_marts содержит %s строк.", count)


with DAG(
    dag_id="3_marts_clickhouse_taxi_data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    params=DEFAULT_PARAMS,
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

    task_verify = PythonOperator(
        task_id="check_mart_rows", 
        python_callable=check_mart_rows
    )

    task_create_ch >> task_create_bridge >> task_insert_via_pxf >> task_verify
