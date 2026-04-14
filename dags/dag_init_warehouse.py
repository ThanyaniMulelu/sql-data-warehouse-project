"""
DAG: Initialize Data Warehouse
================================
One-time manual DAG to create the DataWarehouse database, schemas,
tables, stored procedures, and gold views.

Trigger manually from the Airflow UI — do NOT schedule.
"""

import os
import re
from datetime import datetime
from pathlib import Path

import pymssql
from airflow import DAG
from airflow.operators.python import PythonOperator

SQL_DIR = Path("/opt/airflow/sql")

MSSQL_HOST = os.environ.get("MSSQL_HOST", "mssql")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "")


def _read_sql(relative_path: str) -> str:
    return (SQL_DIR / relative_path).read_text(encoding="utf-8")


def _split_go(sql: str) -> list[str]:
    batches = re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
    return [b.strip() for b in batches if b.strip()]


def _strip_use(sql: str) -> str:
    """Remove USE <database> lines from SQL without removing the rest of the batch."""
    return re.sub(r"^\s*USE\s+\S+;?\s*$", "", sql, flags=re.MULTILINE | re.IGNORECASE).strip()


def _execute_batches(sql_batches: list[str], database: str = "master"):
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
        for batch in sql_batches:
            stripped = _strip_use(batch)
            if not stripped:
                continue
            cursor.execute(stripped)
    finally:
        conn.close()


def create_database(**kwargs):
    init_sql = _read_sql("scripts/init_database.sql")
    batches = _split_go(init_sql)
    master_batches = [
        b for b in batches
        if not re.search(r"(?:^\s*USE\s+\w+|CREATE\s+SCHEMA)", b, re.IGNORECASE | re.MULTILINE)
    ]
    _execute_batches(master_batches, database="master")


def create_schemas(**kwargs):
    init_sql = _read_sql("scripts/init_database.sql")
    batches = _split_go(init_sql)
    schema_batches = [b for b in batches if re.search(r"CREATE\s+SCHEMA", b, re.IGNORECASE)]
    _execute_batches(schema_batches, database="DataWarehouse")


def create_bronze_tables(**kwargs):
    batches = _split_go(_read_sql("scripts/bronze/ddl_bronze.sql"))
    _execute_batches(batches, database="DataWarehouse")


def create_bronze_proc(**kwargs):
    sql = _read_sql("scripts/bronze/proc_load_bronze.sql")
    _execute_batches([sql], database="DataWarehouse")


def create_silver_tables(**kwargs):
    batches = _split_go(_read_sql("scripts/silver/ddl_silver.sql"))
    _execute_batches(batches, database="DataWarehouse")


def create_silver_proc(**kwargs):
    sql = _read_sql("scripts/silver/proc_load_silver.sql")
    _execute_batches([sql], database="DataWarehouse")


def create_gold_views(**kwargs):
    batches = _split_go(_read_sql("scripts/gold/ddl_gold.sql"))
    _execute_batches(batches, database="DataWarehouse")


with DAG(
    dag_id="init_warehouse",
    description="One-time: create DB, schemas, tables, procs, and gold views",
    schedule=None,
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["init", "warehouse"],
) as dag:

    t_create_db = PythonOperator(task_id="create_database", python_callable=create_database)
    t_create_schemas = PythonOperator(task_id="create_schemas", python_callable=create_schemas)
    t_bronze_tables = PythonOperator(task_id="create_bronze_tables", python_callable=create_bronze_tables)
    t_bronze_proc = PythonOperator(task_id="create_bronze_proc", python_callable=create_bronze_proc)
    t_silver_tables = PythonOperator(task_id="create_silver_tables", python_callable=create_silver_tables)
    t_silver_proc = PythonOperator(task_id="create_silver_proc", python_callable=create_silver_proc)
    t_gold_views = PythonOperator(task_id="create_gold_views", python_callable=create_gold_views)

    (
        t_create_db
        >> t_create_schemas
        >> t_bronze_tables
        >> t_bronze_proc
        >> t_silver_tables
        >> t_silver_proc
        >> t_gold_views
    )
