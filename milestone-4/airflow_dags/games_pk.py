import os
import json
import requests
import pandas as pd
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('fetch_and_store_mlb_data',
          default_args=default_args,
          schedule_interval=None)

def upload_to_s3(bucket_name, object_key, data):
    """
    Upload data to an S3 bucket
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=json.dumps(data))
        print(f"Data uploaded to S3: {bucket_name}/{object_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def fetch_and_save_game_data(date, game_pk, bucket):
    print(f"Fetching data for game_pk {game_pk}...")
    api_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            if "error" in data and data["error"] == "Invalid Game PK.":
                return f"Invalid Game PK for game_pk {game_pk}. Skipping."
            
            object_key = f"games/{date.year}/{date.month:02d}/{date.day:02d}/{game_pk}.json"
            upload_to_s3(bucket, object_key, data)
            return f"Data for game_pk {game_pk} uploaded to S3 at {object_key}"
        else:
            return f"Failed to fetch data for game_pk {game_pk}: Status code {response.status_code}"
    except Exception as e:
        return f"Exception for game_pk {game_pk}: {e}"

def fetch_game_data_and_store():
    print("Loading DataFrame from CSV...")
    df = pd.read_csv('./schedule_data.csv')
    print(df.head())
    print("DataFrame loaded. Processing data.")
    bucket = 'mlb-data-store'  # Replace with your S3 bucket name

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_and_save_game_data, pd.to_datetime(row['Date']), row['GamePk'], bucket): row for index, row in df.iterrows()}

    for future in as_completed(futures):
        result = future.result()
        print(result)

    print("Data fetching and storing process completed")

fetch_store_task = PythonOperator(
    task_id='fetch_and_store_mlb_data_task',
    python_callable=fetch_game_data_and_store,
    dag=dag,
)

fetch_store_task
