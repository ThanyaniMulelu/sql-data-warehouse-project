"""
Bootstrap Superset with datasets, charts, and a dashboard for the DataWarehouse.
Run inside the Superset container.
"""
import requests, json, sys, time

BASE = "http://localhost:8088"
s = requests.Session()

# ---------- login ----------
r = s.post(f"{BASE}/api/v1/security/login", json={
    "username": "admin", "password": "admin",
    "provider": "db", "refresh": True
})
token = r.json()["access_token"]
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# get CSRF token
csrf = s.get(f"{BASE}/api/v1/security/csrf_token/", headers=h).json()["result"]
h["X-CSRFToken"] = csrf
h["Referer"] = BASE

# ---------- find database id ----------
dbs = s.get(f"{BASE}/api/v1/database/", headers=h).json()
db_id = None
for d in dbs["result"]:
    if d["database_name"] == "DataWarehouse":
        db_id = d["id"]
        break
if db_id is None:
    print("ERROR: DataWarehouse database not found in Superset"); sys.exit(1)
print(f"DataWarehouse db_id = {db_id}")

# ---------- create datasets (virtual / SQL) ----------
DATASETS = [
    {
        "slug": "sales_by_country",
        "name": "Sales by Country",
        "sql": """
SELECT c.country, COUNT(f.order_number) AS total_orders,
       SUM(f.sales_amount) AS total_sales, SUM(f.quantity) AS total_qty
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
"""
    },
    {
        "slug": "monthly_sales",
        "name": "Monthly Sales Trend",
        "sql": """
SELECT FORMAT(f.order_date, 'yyyy-MM') AS month,
       COUNT(f.order_number) AS orders, SUM(f.sales_amount) AS revenue
FROM gold.fact_sales f
WHERE f.order_date IS NOT NULL
GROUP BY FORMAT(f.order_date, 'yyyy-MM')
"""
    },
    {
        "slug": "top_products",
        "name": "Top 15 Products by Revenue",
        "sql": """
SELECT TOP 15 p.product_name, p.category, p.subcategory,
       SUM(f.sales_amount) AS revenue, SUM(f.quantity) AS units_sold
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.subcategory
ORDER BY revenue DESC
"""
    },
    {
        "slug": "gender_split",
        "name": "Customer Gender Distribution",
        "sql": """
SELECT gender, COUNT(*) AS customer_count
FROM gold.dim_customers
GROUP BY gender
"""
    },
    {
        "slug": "category_revenue",
        "name": "Revenue by Product Category",
        "sql": """
SELECT p.category, p.subcategory,
       SUM(f.sales_amount) AS revenue, COUNT(f.order_number) AS orders
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE p.category IS NOT NULL
GROUP BY p.category, p.subcategory
"""
    },
    {
        "slug": "customer_by_country",
        "name": "Customers per Country",
        "sql": """
SELECT country, COUNT(*) AS customer_count
FROM gold.dim_customers
WHERE country IS NOT NULL
GROUP BY country
"""
    },
    {
        "slug": "kpi_summary",
        "name": "KPI Summary",
        "sql": """
SELECT
  (SELECT COUNT(*) FROM gold.dim_customers) AS total_customers,
  (SELECT COUNT(*) FROM gold.dim_products)  AS total_products,
  (SELECT COUNT(*) FROM gold.fact_sales)    AS total_orders,
  (SELECT SUM(sales_amount) FROM gold.fact_sales) AS total_revenue
"""
    },
]

ds_ids = {}
for ds in DATASETS:
    payload = {
        "database": db_id,
        "schema": "gold",
        "table_name": ds["slug"],
        "sql": ds["sql"].strip(),
    }
    r = s.post(f"{BASE}/api/v1/dataset/", headers=h, json=payload)
    if r.status_code in (201, 200):
        ds_ids[ds["slug"]] = r.json()["id"]
        print(f"  Created dataset: {ds['name']} (id={ds_ids[ds['slug']]})")
    elif "already exists" in r.text.lower():
        # find existing
        existing = s.get(f"{BASE}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:'{ds['slug']}')))", headers=h).json()
        if existing.get("result"):
            ds_ids[ds["slug"]] = existing["result"][0]["id"]
            print(f"  Dataset exists: {ds['name']} (id={ds_ids[ds['slug']]})")
        else:
            print(f"  WARN: could not find existing dataset {ds['slug']}: {r.text}")
    else:
        print(f"  WARN: dataset {ds['slug']}: {r.status_code} {r.text[:200]}")

# ---------- create charts ----------
CHARTS = [
    {
        "name": "Total Revenue by Country",
        "dataset": "sales_by_country",
        "viz_type": "echarts_bar",
        "params": {
            "viz_type": "echarts_bar",
            "x_axis": "country",
            "metrics": [{"label": "Total Sales", "expressionType": "SIMPLE", "column": {"column_name": "total_sales"}, "aggregate": "SUM"}],
            "groupby": [],
            "row_limit": 50,
            "color_scheme": "supersetColors",
            "show_legend": True,
            "x_axis_title": "Country",
            "y_axis_title": "Revenue",
        }
    },
    {
        "name": "Monthly Revenue Trend",
        "dataset": "monthly_sales",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "x_axis": "month",
            "metrics": [{"label": "Revenue", "expressionType": "SIMPLE", "column": {"column_name": "revenue"}, "aggregate": "SUM"}],
            "groupby": [],
            "row_limit": 500,
            "color_scheme": "supersetColors",
            "show_legend": True,
        }
    },
    {
        "name": "Top 15 Products",
        "dataset": "top_products",
        "viz_type": "echarts_bar",
        "params": {
            "viz_type": "echarts_bar",
            "x_axis": "product_name",
            "metrics": [{"label": "Revenue", "expressionType": "SIMPLE", "column": {"column_name": "revenue"}, "aggregate": "SUM"}],
            "groupby": [],
            "row_limit": 15,
            "color_scheme": "supersetColors",
            "orient": "horizontal",
        }
    },
    {
        "name": "Customer Gender Split",
        "dataset": "gender_split",
        "viz_type": "pie",
        "params": {
            "viz_type": "pie",
            "groupby": ["gender"],
            "metrics": [{"label": "Count", "expressionType": "SIMPLE", "column": {"column_name": "customer_count"}, "aggregate": "SUM"}],
            "color_scheme": "supersetColors",
            "show_legend": True,
            "show_labels": True,
        }
    },
    {
        "name": "Revenue by Category",
        "dataset": "category_revenue",
        "viz_type": "treemap_v2",
        "params": {
            "viz_type": "treemap_v2",
            "groupby": ["category", "subcategory"],
            "metrics": [{"label": "Revenue", "expressionType": "SIMPLE", "column": {"column_name": "revenue"}, "aggregate": "SUM"}],
            "color_scheme": "supersetColors",
        }
    },
    {
        "name": "Customers per Country",
        "dataset": "customer_by_country",
        "viz_type": "pie",
        "params": {
            "viz_type": "pie",
            "groupby": ["country"],
            "metrics": [{"label": "Customers", "expressionType": "SIMPLE", "column": {"column_name": "customer_count"}, "aggregate": "SUM"}],
            "color_scheme": "supersetColors",
            "show_legend": True,
            "donut": True,
        }
    },
]

chart_ids = []
for ch in CHARTS:
    ds_id_val = ds_ids.get(ch["dataset"])
    if not ds_id_val:
        print(f"  SKIP chart '{ch['name']}' — missing dataset '{ch['dataset']}'")
        continue
    payload = {
        "slice_name": ch["name"],
        "datasource_id": ds_id_val,
        "datasource_type": "table",
        "viz_type": ch["viz_type"],
        "params": json.dumps(ch["params"]),
    }
    r = s.post(f"{BASE}/api/v1/chart/", headers=h, json=payload)
    if r.status_code in (200, 201):
        cid = r.json()["id"]
        chart_ids.append(cid)
        print(f"  Created chart: {ch['name']} (id={cid})")
    else:
        print(f"  WARN chart '{ch['name']}': {r.status_code} {r.text[:200]}")

# ---------- create dashboard ----------
if chart_ids:
    # Build a simple grid layout
    positions = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]}}
    positions["GRID_ID"] = {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]}
    positions["HEADER_ID"] = {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": "Data Warehouse Analytics"}}

    row_children = []
    for i, cid in enumerate(chart_ids):
        comp_id = f"CHART-{cid}"
        positions[comp_id] = {
            "type": "CHART",
            "id": comp_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", f"ROW-{i}"],
            "meta": {"width": 6, "height": 50, "chartId": cid, "sliceName": ""},
        }
        row_id = f"ROW-{i}"
        positions[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [comp_id],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        positions["GRID_ID"]["children"].append(row_id)

    payload = {
        "dashboard_title": "Data Warehouse Analytics",
        "published": True,
        "slug": "dw-analytics",
        "position_json": json.dumps(positions),
    }
    r = s.post(f"{BASE}/api/v1/dashboard/", headers=h, json=payload)
    if r.status_code in (200, 201):
        dash_id = r.json()["id"]
        print(f"\nDashboard created (id={dash_id}): http://localhost:8088/superset/dashboard/{dash_id}/")
    else:
        print(f"WARN dashboard: {r.status_code} {r.text[:300]}")
else:
    print("No charts created — skipping dashboard.")

print("\nDone!")
