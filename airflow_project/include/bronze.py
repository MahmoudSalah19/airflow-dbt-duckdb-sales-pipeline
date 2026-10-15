import os
from pathlib import Path
import duckdb

# Inside the container, AIRFLOW_HOME is /usr/local/airflow
BASE_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
DB_PATH = BASE_DIR / "include" / "dev.duckdb"
RAW_PATH = BASE_DIR / "include" / "data"

TABLES = {
    "accounts": "accounts.csv",
    "products": "products.csv",
    "sales_pipeline": "sales_pipeline.csv",
    "sales_team": "sales_teams.csv",
    "data_dictionary": "data_dictionary.csv",
}


def load_bronze():
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS main_bronze")
        for table_name, file_name in TABLES.items():
            csv_path = RAW_PATH / file_name
            con.execute(f"""
                CREATE OR REPLACE TABLE main_bronze.{table_name} AS
                SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=True)
            """)
            count = con.execute(
                f"SELECT COUNT(*) FROM main_bronze.{table_name}"
            ).fetchone()[0]
            print(f"Loaded main_bronze.{table_name}: {count} rows")
    finally:
        con.close()
    print("BRONZE load complete")


if __name__ == "__main__":
    load_bronze()