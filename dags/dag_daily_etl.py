"""
DAG: Daily ETL Pipeline
========================
Runs daily: Bronze load → Silver load → Quality checks (silver → gold).
Quality check failures halt the pipeline.
"""

import os
from datetime import datetime, timedelta

import pymssql
from airflow import DAG
from airflow.operators.python import PythonOperator

MSSQL_HOST = os.environ.get("MSSQL_HOST", "mssql")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _exec_sql(sql: str, database: str = "DataWarehouse"):
    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=database,
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
    finally:
        conn.close()


def load_bronze(**kwargs):
    _exec_sql("EXEC bronze.load_bronze;")


def load_silver(**kwargs):
    _exec_sql("EXEC silver.load_silver;")


def run_silver_quality_checks(**kwargs):
    from sql_quality_checks import run_quality_checks
    run_quality_checks(layer="silver")


def run_gold_quality_checks(**kwargs):
    from sql_quality_checks import run_quality_checks
    run_quality_checks(layer="gold")


with DAG(
    dag_id="daily_etl",
    description="Daily: load bronze → silver, then run quality checks",
    schedule="@daily",
    start_date=datetime(2026, 4, 14),
    catchup=False,
    default_args=default_args,
    tags=["etl", "warehouse", "daily"],
) as dag:

    t_load_bronze = PythonOperator(task_id="load_bronze", python_callable=load_bronze)
    t_load_silver = PythonOperator(task_id="load_silver", python_callable=load_silver)
    t_qc_silver = PythonOperator(task_id="quality_checks_silver", python_callable=run_silver_quality_checks)
    t_qc_gold = PythonOperator(task_id="quality_checks_gold", python_callable=run_gold_quality_checks)

    t_load_bronze >> t_load_silver >> t_qc_silver >> t_qc_gold
