import os
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import datetime
import json

with DAG(
        dag_id="gcp_test",
        start_date=datetime.datetime(2023, 7, 26),
        tags=["lecture"],
        catchup=True
) as dag:
    load_from_local = LocalFilesystemToGCSOperator(
        task_id="load_from_local",
        src="/opt/airflow/data/*",
        dst="airflow/load/data/",
        bucket="de-07-2023-ag"
    )
