import os
import json
import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Environment variables
API_URL = os.getenv("API_URL") 
API_KEY = os.getenv("API_KEY")
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST") ,
    "user": os.getenv("MYSQL_USER") ,
    "password": os.getenv("MYSQL_PASSWORD"), 
    "database": os.getenv("MYSQL_DB")
}

# Default DAG arguments
default_args = {
    "owner": "tshipa",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def fetch_data(**kwargs):
    start_date = "2023-01-01"
    end_date = "2023-01-31"
    headers = {"x-api-key": API_KEY}
    payload = json.dumps({"start_date": start_date, "end_date": end_date})
    
    response = requests.post(API_URL, headers=headers, data=payload)
    response.raise_for_status()
    
    df = pd.DataFrame(response.json())
    kwargs["ti"].xcom_push(key="raw_data", value=df.to_json())

def transform_data(**kwargs):
   
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="fetch_data", key="raw_data")
    
    df = pd.read_json(raw_data)
    df.fillna(value={col: "Unknown" if df[col].dtype == "object" else 0 for col in df.columns}, inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[df["transaction_amount"] >= 0]
    
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
     # Ensure transaction_date is in correct format
    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], unit="ms")
        df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    df["transaction_category"] = pd.cut(
        df["transaction_amount"],
        bins=[0, 50, 200, float("inf")],
        labels=["low", "medium", "high"]
    )
    
    df["total_per_customer"] = df.groupby("customer_id")["transaction_amount"].transform("sum")
    
    ti.xcom_push(key="transformed_data", value=df.to_json())


def load_data(**kwargs):
   
    ti = kwargs["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data", key="transformed_data")

    # Convert JSON string to DataFrame
    df = pd.read_json(transformed_data)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Drop table if exists and create a new one
    cursor.execute("DROP TABLE IF EXISTS transactions;")
    cursor.execute("""
        CREATE TABLE transactions (
            customer_id VARCHAR(255),
            product_id VARCHAR(255),
            transaction_date DATETIME,
            transaction_amount FLOAT,
            transaction_type VARCHAR(255),
            spend_category VARCHAR(255),
            product_category VARCHAR(255),
            transaction_category VARCHAR(50),
            total_per_customer FLOAT
        );
    """)
    conn.commit()

    # Insert data
    table_name = "transactions"
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    cursor.executemany(sql, values)
    conn.commit()

    # Close connections
    cursor.close()
    conn.close()


fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

fetch_task >> transform_task >> load_task
