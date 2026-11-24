from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/usr/local/airflow/include')

default_args = {
    'owner': 'harsha',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_fetch_and_store():
    from fetch_sp500 import fetch_market_data, compute_sector_summary, write_parquet_to_s3
    market_df = fetch_market_data()
    write_parquet_to_s3(market_df, 'sp500_daily')
    return f"Fetched {len(market_df)} rows"

def run_sector_summary():
    from fetch_sp500 import fetch_market_data, compute_sector_summary, write_parquet_to_s3
    market_df = fetch_market_data()
    sector_df = compute_sector_summary(market_df)
    write_parquet_to_s3(sector_df, 'sector_summary')
    return f"Summary: {len(sector_df)} sectors"

with DAG(
    dag_id='sp500_daily_pipeline',
    default_args=default_args,
    description='Daily S&P 500 data pipeline with sector analytics and anomaly detection',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['sp500', 'finance', 'aws', 's3'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_market_data',
        python_callable=run_fetch_and_store,
    )

    summary_task = PythonOperator(
        task_id='compute_sector_summary',
        python_callable=run_sector_summary,
    )

    fetch_task >> summary_task