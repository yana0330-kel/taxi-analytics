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
GP_CONN_ID = "greenplum_dwh"

with DAG(
    dag_id="extract_core_taxi_data",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["taxi-project", "core"],
) as dag:
