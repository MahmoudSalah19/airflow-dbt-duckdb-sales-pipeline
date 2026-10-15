# Sales CRM Data Warehouse: Airflow + dbt + DuckDB

An end-to-end data pipeline that loads raw CRM sales CSVs, cleans them, models them as a star schema, tests the result, and orchestrates everything with Apache Airflow. dbt models run as individual Airflow tasks through Astronomer Cosmos, and the data is stored in a single DuckDB file.

## Architecture

```
CSV files  ->  Bronze (raw)  ->  Silver (cleaned)  ->  Gold (star schema)
                 main_bronze        main_silver           main_gold
```

| Layer | What it holds | Built by |
| --- | --- | --- |
| Bronze | Unmodified copy of 5 CSV files | `include/bronze.py` (Python, DuckDB `read_csv_auto`) |
| Silver | 4 cleaned models: accounts, products, sales_pipeline, sales_team | dbt |
| Gold | 1 fact table and 4 dimensions | dbt |

Airflow runs the layers in order: `load_bronze`, then the silver models and tests, then the gold models and tests.

## Tech stack

- **Apache Airflow 2** on Astro Runtime 12.6.0 (Astro CLI, Docker)
- **dbt-core** with the **dbt-duckdb** adapter
- **Astronomer Cosmos** to turn each dbt model and test into an Airflow task
- **DuckDB** as the warehouse

## Data model

The gold layer is a star schema. Each row in `fct_sales_opportunity` is one sales opportunity.

| Table | Grain | Key |
| --- | --- | --- |
| `fct_sales_opportunity` | One row per sales opportunity | `opportunity_id` |
| `dim_accounts` | One row per company | `account_id` |
| `dim_products` | One row per product | `product_id` |
| `dim_sales_team` | One row per sales agent | `agent_id` |
| `dim_dates` | One row per date (used twice: engage date and close date) | `date_key` |

Design notes:

- Surrogate keys are generated with `ROW_NUMBER() OVER (ORDER BY name)`, so they are rebuilt on every run.
- `close_date_key` is NULL for open deals (Prospecting and Engaging).
- Foreign keys, uniqueness and not-null rules are enforced by dbt tests, not by DuckDB constraints.

### Conceptual model

![Conceptual model](data%20models/conceptual%20model.png)

### Logical model

![Logical model](data%20models/logical%20model.png)

### Physical model

![Physical model](data%20models/physical%20model.png)

## Data quality

| Issue in the source | Fix |
| --- | --- |
| Sector misspelled as `technolgy` | Corrected in silver |
| Product `GTXPro` instead of `GTX Pro` | Standardised in silver so joins match |
| Open deals have no close date | Kept as NULL and documented |

## Testing

The project has 45 dbt data tests.

- **Generic tests** in each layer's `schema.yml`: `unique`, `not_null`, `relationships`, `accepted_values`.
- **Singular tests** in `tests/silver` and `tests/gold`: business rules such as close date after engage date, Won deals have a close date, and fact row count equals pipeline row count.

Gold singular tests reference silver models, so the silver step excludes `path:tests/gold`. Otherwise it would run them before the gold layer exists.

## Orchestration

Two DAGs run the same pipeline:

| DAG | Approach |
| --- | --- |
| `bronze_dbt_bashoperator` | `BashOperator` tasks: one run and one test task per layer |
| `bronze_dbt_cosmos` | Cosmos `DbtTaskGroup` for silver and gold: one task per model and per test |

Both use `max_active_tasks=1` because DuckDB allows a single writer. Do not run both DAGs at the same time.

The Cosmos DAG reads `target/manifest.json` instead of running `dbt ls` at parse time, which avoids a `DagBag import timeout`. Run `dbt parse` again after you change any model, test or `ref()`.

dbt is installed in an isolated virtual environment (`dbt_venv`) inside the Docker image, so its dependencies never conflict with Airflow's.

## Project structure

```
.
├── airflow_project/
│   ├── dags/
│   │   ├── bashoperator_dbt_dag.py
│   │   └── cosmos_dbt_dag.py
│   ├── include/
│   │   ├── bronze.py
│   │   ├── data/                     source CSV files
│   │   └── dbt/dbt_project/
│   │       ├── models/silver/
│   │       ├── models/gold/
│   │       ├── tests/silver/ and tests/gold/
│   │       └── profiles.yml          targets: dev and local
│   ├── Dockerfile                    builds dbt_venv
│   └── requirements.txt
├── data models/                      conceptual, logical, physical diagrams
└── documentation/                    full project documentation
```

## Getting started

### Prerequisites

- Docker Desktop (running, with 6 to 8 GB of memory)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Python 3.10 or newer

### 1. Clone the repository

```bash
git clone https://github.com/MahmoudSalah19/airflow-dbt-duckdb-sales-pipeline.git
cd airflow-dbt-duckdb-sales-pipeline
```

### 2. Set up dbt locally

The Cosmos DAG needs a manifest, which is not stored in Git. Generate it once:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install dbt-duckdb
cd airflow_project\include\dbt\dbt_project
dbt parse --profiles-dir . --target local
```

### 3. Start Airflow

```powershell
cd ..\..\..
astro dev start --wait 10m
```

The first start builds the Docker image and can take several minutes.

### 4. Run the pipeline

1. Open `http://localhost:8080` (default login `admin` / `admin`).
2. Trigger `bronze_dbt_cosmos`.
3. Watch the `load_bronze`, `silver` and `gold` groups run in the Graph view.

The results are written to `airflow_project/include/dev.duckdb`. Close any other tool that has this file open before a run, or DuckDB reports a file lock.

### Useful commands

| Task | Command (from `airflow_project/include/dbt/dbt_project`, `.venv` active) |
| --- | --- |
| Run all tests locally | `dbt test --profiles-dir . --target local` |
| Preview a selector | `dbt ls --select "path:models/gold" --profiles-dir . --target local` |
| Build dbt docs | `dbt docs generate --profiles-dir . --target local` |
| Serve dbt docs | `dbt docs serve --profiles-dir . --target local --port 8081` |

## Documentation

The full documentation, including setup details and a troubleshooting log, is in the [documentation](documentation/Documentation.md) folder ([PDF](documentation/Documentation.pdf)).

## Possible improvements

- Build `dim_dates` as a full calendar so there are no missing days.
- Add an Unknown member to each dimension for open deals.
- Replace the parent company name with a `parent_account_id` that points back to `dim_accounts`.
- Use incremental models once data arrives continuously.
- Run `dbt test` and `dbt parse` in GitHub Actions on every push.
- Move to PostgreSQL, BigQuery or Snowflake for a production warehouse.
