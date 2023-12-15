import os
import datetime
import requests
import boto3
import sqlalchemy
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.today(),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('baseball_schedule_fetcher_2', default_args=default_args, schedule_interval=None)

def upload_to_s3(bucket_name, object_key, data):
    """
    Upload a file to an S3 bucket
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=json.dumps(data))
        print(f"File uploaded to S3: {object_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def fetch_schedule_data(date):
    schedule_url = f"https://baseballsavant.mlb.com/schedule?date={date.strftime('%Y-%m-%d')}"
    schedule_response = requests.get(schedule_url)
    data = []

    if schedule_response.status_code == 200 and schedule_response.json():
        try:
            schedule_data = schedule_response.json()

            # Extracting gamePk values from the schedule data
            if 'schedule' in schedule_data and 'dates' in schedule_data['schedule']:
                for date_entry in schedule_data['schedule']['dates']:
                    for game in date_entry['games']:
                        game_pk = game.get('gamePk')
                        if game_pk:
                            data.append([date, game_pk])
            # Upload JSON response directly to S3
            s3_path = f"schedules/{date.year}/{date.month}/{date.day}.json"
            upload_to_s3('mlb-data-store', s3_path, schedule_data)
        except Exception as e:
            print(f"Error processing schedule data for date: {date}: {e}")
    else:
        # Log a message if the response is not successful or if there's no JSON data
        print(f"No data or unsuccessful response for date: {date}. Status code: {schedule_response.status_code}, Response: {schedule_response.text}")
    
    return data

def fetch_and_store_data(ds, **kwargs):
    print("Starting data fetch and store process")

    start_date = datetime.date(1901, 1, 1)
    end_date = datetime.date.today()
    print(f"Fetching data from {start_date} to {end_date}")

    dates = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    futures = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for date in dates:
            futures.append(executor.submit(fetch_schedule_data, date))
            print(f"Scheduled data fetch for date: {date}")

    print("Waiting for data fetching tasks to complete")
    results = []
    for future in as_completed(futures):
        results.extend(future.result())

    print("Data fetching completed. Preparing to save data to CSV")
    # Convert results to DataFrame and save as CSV
    df = pd.DataFrame(results, columns=['Date', 'GamePk'])
    df.insert(0, 'SerialNumber', range(1, 1 + len(df)))
    csv_file_path = './schedule_data.csv'
    df.to_csv(csv_file_path, index=False)
    print(f"Data saved to CSV at {csv_file_path}")

    # Read and print the CSV file
    df = pd.read_csv(csv_file_path)
    print("CSV Data:")
    print(df)

    # Store data in PostgreSQL
    # Replace with your PostgreSQL connection details
    postgres_connection = 'postgresql+psycopg2://airflow:airflow@localhost/airflow'
    engine = sqlalchemy.create_engine(postgres_connection)
    try:
        df.to_sql('schedule_data', engine, index=False, if_exists='replace')
        print("Data stored in PostgreSQL successfully")
    except Exception as e:
        print(f"Error storing data in PostgreSQL: {e}")

    # Upload the CSV file to S3
    s3_csv_path = f"schedules/schedule_data.csv"
    print(f"Uploading CSV to S3 at {s3_csv_path}")
    s3_client = boto3.client('s3')
    s3_client.upload_file(csv_file_path, 'mlb-data-store', s3_csv_path)
    print("CSV upload to S3 completed")

# Task definition
fetch_store_task = PythonOperator(
    task_id='fetch_and_store_schedule_data',
    provide_context=True,
    python_callable=fetch_and_store_data,
    dag=dag,
)

fetch_store_task

