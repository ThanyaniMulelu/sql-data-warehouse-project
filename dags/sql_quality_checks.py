"""
Quality Check Runner
=====================
Executes data quality SQL checks against the silver and gold layers.
Each check is a SELECT that should return ZERO rows on success.
Any rows returned indicate a data quality issue and raise AirflowFailException.
"""

import logging
import os

import pymssql
from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

MSSQL_HOST = os.environ.get("MSSQL_HOST", "mssql")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "")

SILVER_CHECKS = [
    {
        "name": "crm_cust_info: NULL or duplicate PK",
        "sql": """
            SELECT cst_id, COUNT(*)
            FROM silver.crm_cust_info
            GROUP BY cst_id
            HAVING COUNT(*) > 1 OR cst_id IS NULL;
        """,
    },
    {
        "name": "crm_cust_info: untrimmed cst_key",
        "sql": """
            SELECT cst_key
            FROM silver.crm_cust_info
            WHERE cst_key != TRIM(cst_key);
        """,
    },
    {
        "name": "crm_prd_info: NULL or duplicate PK",
        "sql": """
            SELECT prd_id, COUNT(*)
            FROM silver.crm_prd_info
            GROUP BY prd_id
            HAVING COUNT(*) > 1 OR prd_id IS NULL;
        """,
    },
    {
        "name": "crm_prd_info: untrimmed prd_nm",
        "sql": """
            SELECT prd_nm
            FROM silver.crm_prd_info
            WHERE prd_nm != TRIM(prd_nm);
        """,
    },
    {
        "name": "crm_prd_info: NULL or negative cost",
        "sql": """
            SELECT prd_cost
            FROM silver.crm_prd_info
            WHERE prd_cost < 0 OR prd_cost IS NULL;
        """,
    },
    {
        "name": "crm_prd_info: start_date > end_date",
        "sql": """
            SELECT *
            FROM silver.crm_prd_info
            WHERE prd_end_dt < prd_start_dt;
        """,
    },
    {
        "name": "crm_sales_details: invalid date order",
        "sql": """
            SELECT *
            FROM silver.crm_sales_details
            WHERE sls_order_dt > sls_ship_dt
               OR sls_order_dt > sls_due_dt;
        """,
    },
    {
        "name": "crm_sales_details: sales != qty * price",
        "sql": """
            SELECT sls_sales, sls_quantity, sls_price
            FROM silver.crm_sales_details
            WHERE sls_sales != sls_quantity * sls_price
               OR sls_sales IS NULL
               OR sls_quantity IS NULL
               OR sls_price IS NULL
               OR sls_sales <= 0
               OR sls_quantity <= 0
               OR sls_price <= 0;
        """,
    },
    {
        "name": "erp_cust_az12: birthdate out of range",
        "sql": """
            SELECT bdate
            FROM silver.erp_cust_az12
            WHERE bdate < '1924-01-01'
               OR bdate > GETDATE();
        """,
    },
    {
        "name": "erp_px_cat_g1v2: untrimmed fields",
        "sql": """
            SELECT *
            FROM silver.erp_px_cat_g1v2
            WHERE cat != TRIM(cat)
               OR subcat != TRIM(subcat)
               OR maintenance != TRIM(maintenance);
        """,
    },
]

GOLD_CHECKS = [
    {
        "name": "dim_customers: duplicate customer_key",
        "sql": """
            SELECT customer_key, COUNT(*) AS duplicate_count
            FROM gold.dim_customers
            GROUP BY customer_key
            HAVING COUNT(*) > 1;
        """,
    },
    {
        "name": "dim_products: duplicate product_key",
        "sql": """
            SELECT product_key, COUNT(*) AS duplicate_count
            FROM gold.dim_products
            GROUP BY product_key
            HAVING COUNT(*) > 1;
        """,
    },
    {
        "name": "fact_sales: orphaned foreign keys",
        "sql": """
            SELECT f.*
            FROM gold.fact_sales f
            LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
            LEFT JOIN gold.dim_products p  ON p.product_key  = f.product_key
            WHERE p.product_key IS NULL OR c.customer_key IS NULL;
        """,
    },
]


def run_quality_checks(layer: str) -> None:
    checks = SILVER_CHECKS if layer == "silver" else GOLD_CHECKS

    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database="DataWarehouse",
        autocommit=True,
    )

    failures = []
    try:
        cursor = conn.cursor()
        for check in checks:
            log.info("Running check: %s", check["name"])
            cursor.execute(check["sql"])
            records = cursor.fetchall()

            if records:
                msg = f"FAILED: {check['name']} — returned {len(records)} row(s)"
                log.error(msg)
                failures.append(msg)
            else:
                log.info("PASSED: %s", check["name"])
    finally:
        conn.close()

    if failures:
        summary = "\n".join(failures)
        raise AirflowFailException(
            f"{len(failures)} quality check(s) failed for {layer} layer:\n{summary}"
        )

    log.info("All %d %s quality checks passed.", len(checks), layer)
