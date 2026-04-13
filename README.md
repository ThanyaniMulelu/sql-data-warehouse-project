# SQL Data Warehouse Project

A SQL Server data warehouse built using the **Medallion Architecture** (Bronze → Silver → Gold), integrating customer, product, and sales data from CRM and ERP source systems into a star schema optimized for analytics and reporting.

---

## Architecture

```
          ┌──────────┐     ┌──────────┐     ┌──────────┐
CSV Files │  Bronze   │────▶│  Silver   │────▶│   Gold   │
  ────▶   │ (Raw Data)│     │(Cleansed) │     │(Analytics)│
          └──────────┘     └──────────┘     └──────────┘
```

| Layer | Purpose | Implementation |
|-------|---------|----------------|
| **Bronze** | Raw data landing zone | Bulk-loaded CSV files, no transformations |
| **Silver** | Cleansed and standardized data | Stored procedures with deduplication, normalization, validation |
| **Gold** | Business-ready analytics | Star schema views (dimensions + fact tables) |

---

## Data Model (Gold Layer)

```
          ┌────────────────┐
          │ dim_customers   │
          │────────────────│
          │ customer_key    │◄──┐
          │ customer_id     │   │
          │ first_name      │   │
          │ last_name       │   │
          │ country         │   │
          │ marital_status  │   │
          │ gender          │   │
          │ birthdate       │   │
          └────────────────┘   │
                               │
          ┌────────────────┐   │    ┌────────────────┐
          │  fact_sales     │   │    │ dim_products    │
          │────────────────│   │    │────────────────│
          │ order_number    │   │    │ product_key     │◄──┐
          │ product_key  ───│───┼──▶│ product_id      │   │
          │ customer_key ───│───┘    │ product_name    │   │
          │ order_date      │        │ category        │   │
          │ shipping_date   │        │ subcategory     │   │
          │ due_date        │        │ product_cost    │   │
          │ sales_amount    │        │ product_line    │   │
          │ quantity         │        └────────────────┘   │
          │ price           │                              │
          └────────────────┘                              │
```

---

## Data Sources

| Source | File | Description |
|--------|------|-------------|
| CRM | `cust_info.csv` | Customer master data (~11K records) |
| CRM | `prd_info.csv` | Product information with cost and line details |
| CRM | `sales_details.csv` | Sales transactions (2010–2011) |
| ERP | `CUST_AZ12.csv` | Customer demographics (birthdate, gender) |
| ERP | `LOC_A101.csv` | Customer-to-country mapping |
| ERP | `PX_CAT_G1V2.csv` | Product categories and subcategories |

---

## Project Structure

```
sql-data-warehouse-project/
│
├── datasets/                 # Source CSV files
│   ├── source_crm/           # CRM system exports
│   └── source_erp/           # ERP system exports
│
├── docs/                     # Documentation
│   ├── data_catalog.md       # Data dictionary and catalog
│   └── naming_conventions.md # Naming standards
│
├── scripts/                  # SQL scripts
│   ├── init_database.sql     # Database and schema creation
│   ├── bronze/
│   │   ├── ddl_bronze.sql          # Bronze table definitions
│   │   └── proc_load_bronze.sql    # Bronze loading procedure
│   ├── silver/
│   │   ├── ddl_silver.sql          # Silver table definitions
│   │   └── proc_load_silver.sql    # Silver loading procedure
│   └── gold/
│       └── ddl_gold.sql            # Gold layer views
│
└── tests/                    # Data quality checks
    ├── quality_checks_silver.sql   # Silver layer validations
    └── quality_checks_gold.sql     # Gold layer validations
```

---

## Key Transformations (Silver Layer)

- **Deduplication** — Window functions to select most recent records
- **Gender normalization** — `M`/`F` → `Male`/`Female`
- **Marital status** — `S`/`M` → `Single`/`Married`
- **Product line expansion** — `M`/`R`/`S`/`T` → `Mountain`/`Road`/`Other Sales`/`Touring`
- **Date conversion** — Integer `YYYYMMDD` → proper `DATE` type
- **Country code mapping** — `DE`/`US` → `Germany`/`United States`
- **Data validation** — Future birthdates removed, sales recalculated where inconsistent
- **ID cleanup** — `NAS` prefix stripped from customer IDs

---

## Getting Started

### Prerequisites

- **SQL Server** (2016 or later)
- **SQL Server Management Studio (SSMS)** or any SQL client

### Setup

1. **Create the database and schemas:**
   ```sql
   -- Run init_database.sql
   ```
2. **Create Bronze tables and load raw data:**
   ```sql
   -- Run scripts/bronze/ddl_bronze.sql
   -- Run scripts/bronze/proc_load_bronze.sql
   EXEC bronze.load_bronze;
   ```
3. **Create Silver tables and load cleansed data:**
   ```sql
   -- Run scripts/silver/ddl_silver.sql
   -- Run scripts/silver/proc_load_silver.sql
   EXEC silver.load_silver;
   ```
4. **Create Gold layer views:**
   ```sql
   -- Run scripts/gold/ddl_gold.sql
   ```
5. **Run quality checks:**
   ```sql
   -- Run tests/quality_checks_silver.sql
   -- Run tests/quality_checks_gold.sql
   ```

> **Note:** Update the file paths in `proc_load_bronze.sql` to match your local CSV file locations before running the Bronze load.

---

## Data Quality Checks

| Layer | Checks |
|-------|--------|
| **Silver** | Null/duplicate key detection, whitespace validation, date range checks, business rule verification (sales = qty × price) |
| **Gold** | Surrogate key uniqueness, referential integrity between facts and dimensions, orphaned record detection |

---

## Technologies

- SQL Server (T-SQL)
- Medallion Architecture (Bronze/Silver/Gold)
- Star Schema (Kimball methodology)
- BULK INSERT for CSV ingestion
- Window functions, CTEs, TRY-CATCH error handling

---

## Author

**Thanyani Selby Mulelu**  
GitHub: [@ThanyaniMulelu](https://github.com/ThanyaniMulelu)

---

## License

This project is open source and available for learning and educational purposes.
