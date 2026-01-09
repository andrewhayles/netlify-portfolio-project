from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import os
import time

# --- CONFIGURATION ---
NETLIFY_TOKEN = "nfp_Mh6..."  # (Actual token acquired from Netlify website)
SITE_ID = "1a..." # (Actual Site ID acquired from Netlify website)
OUTPUT_PATH = "/tmp/netlify_logs.json"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_netlify_logs():
    """Fetches logs and saves to local JSON"""
    url = f"https://api.netlify.com/api/v1/sites/{SITE_ID}/deploys"
    headers = {"Authorization": f"Bearer {NETLIFY_TOKEN}"}
    
    # (Keeping your existing logic short for brevity)
    all_deploys = []
    page = 1
    while True:
        response = requests.get(url, headers=headers, params={'page': page, 'per_page': 20})
        data = response.json()
        if not data: break
        all_deploys.extend(data)
        if len(data) < 20: break
        page += 1
    
    # Save to JSON
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(all_deploys, f)
    print(f"Saved {len(all_deploys)} logs to {OUTPUT_PATH}")

def load_to_snowflake():
    """
    1. Uploads the file from local Linux /tmp/ to Snowflake Stage
    2. Copies data from Stage into the Table
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # 1. PUT command: Uploads local file to the Table's internal stage
    # @%Table_Name references the implicit stage for that table
    put_query = f"PUT file://{OUTPUT_PATH} @%NETLIFY_LOGS AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    hook.run(put_query)
    
    # 2. COPY command: Loads data from stage into the table
    copy_query = """
    COPY INTO NETLIFY_LOGS (RAW_DATA)
    FROM @%NETLIFY_LOGS
    FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
    """
    hook.run(copy_query)
    print("Data successfully loaded into Snowflake.")

with DAG(
    'netlify_build_ingestion',
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='fetch_netlify_builds',
        python_callable=fetch_netlify_logs,
    )

    t2 = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=load_to_snowflake,
    )

    t1 >> t2