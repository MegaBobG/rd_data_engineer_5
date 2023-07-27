import os
import pandas as pd

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models.variable import Variable

from ht_template.job1.bll.sales_api import save_sales_to_local_disk
from ht_template.job2.bll.transfer_api import transfer_from_raw_to_stg

import datetime

AUTH_TOKEN = Variable.get(key='AUTH_TOKEN')
BASE_DIR = Variable.get(key='BASE_DIR')

start_date = pd.to_datetime("2022-08-09")
end_date = pd.to_datetime("2022-08-11")

BASE_DIR_2 = os.getcwd()
RAW_DIR_2 = os.path.join(BASE_DIR_2, "data", "raw", "sales")
STG_DIR_2 = os.path.join(BASE_DIR_2, "data", "stg", "sales")

dag = DAG(
    dag_id="load_dag_gcs_api",
    schedule_interval="0 1 * * *",
    start_date=datetime.datetime(2023, 7, 26),
    tags=["lecture"],
    max_active_runs=1,
    catchup=True
)

date_range = pd.date_range(start=start_date, end=end_date)
date_str_list = date_range.strftime("%Y-%m-%d").tolist()

def extract_data_from_api_task(*date_str_list, **kwargs):
    for date_str in date_str_list:
        file_name = f"sales_{date_str}.json"
        file_path = os.path.join(RAW_DIR_2, file_name)
        save_sales_to_local_disk(date=date_str, file_path=file_path)

extract_data_from_api = PythonOperator(
    task_id="extract_data_from_api",
    python_callable=extract_data_from_api_task,
    op_args=date_str_list,
    provide_context=True,
    dag=dag
)

convert_to_avro = PythonOperator(
    task_id="convert_to_avro",
    python_callable=transfer_from_raw_to_stg,
    op_kwargs={"raw_dir": RAW_DIR_2, "stg_dir": STG_DIR_2},
    dag=dag
)

load_from_local_raw = LocalFilesystemToGCSOperator(
    task_id="load_from_local_raw",
    src="/opt/airflow/data/raw/sales/*",
    dst="airflow/load/data/raw/sales/",
    bucket="de-07-2023-ag",
    dag=dag
)

load_from_local_stg = LocalFilesystemToGCSOperator(
    task_id="load_from_local_stg",
    src="/opt/airflow/data/stg/sales/*",
    dst="airflow/load/data/stg/sales/",
    bucket="de-07-2023-ag",
    dag=dag
)

# extract_data_from_api >> [load_from_local_raw, convert_to_avro] >> load_from_local_stg

extract_data_from_api >> [load_from_local_raw, convert_to_avro]
convert_to_avro.set_downstream(load_from_local_stg)
