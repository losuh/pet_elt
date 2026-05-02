import shutil
import logging
import pendulum
import os
import zipfile
import re

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
# Вместо airflow.providers.clickhouse...
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

@task
def get_path():
    context = get_current_context()
    exec_date = context["ds_nodash"][:6]
    path = f"http://minio:9000/{BUCKET_NAME_SOURCE}/raw/citibike_data/{exec_date}/*.csv"
    return path


BUCKET_NAME_SOURCE = "bucket"

AWS_CONN_ID_SOURCE = "MINIO"

CH_CONN_ID = "CLICKHOUSE"

CSV_RE = re.compile(r"\d{6}-citibike-tripdata(_\d+)?\.csv$")

default_args = {
    'retries': 3,
    'catchup' : True,
}

@dag(
    dag_id='st2_load_to_clickhouse_dag',
    schedule="0 0 L * *",
    start_date=pendulum.datetime(2024, 12, 16, tz="Europe/Moscow"),
    max_active_runs=1,
    default_args=default_args,
)
def s3_dag():

    sensor = ExternalTaskSensor(
        task_id='wait_for_st1',
        external_dag_id='st1_load_to_s3_dag',
        external_task_id='mark_st2_dependency',
    )

    #list_keys_source = S3ListOperator(
    #    task_id="list_keys_source",
    #    bucket=BUCKET_NAME_SOURCE,
    #    aws_conn_id=AWS_CONN_ID_SOURCE,
    #)



    create_table = ClickHouseOperator(
        clickhouse_conn_id = CH_CONN_ID,
        task_id='create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS trips (
                ride_id String,
                rideable_type String,
                started_at DateTime64(3),
                ended_at DateTime64(3),
                start_station_name String,
                start_station_id String,
                end_station_name String,
                end_station_id String,
                start_lat Float64,
                start_lng Float64,
                end_lat Float64,
                end_lng Float64,
                member_casual String
            ) ENGINE = MergeTree()
            ORDER BY started_at;
            """
    )
    s3_path = get_path()

    # 2. Загружаем данные напрямую из S3
    # Используем функцию s3(), где указываем путь, формат и структуру
    load_from_s3 = ClickHouseOperator(
        task_id='load_from_s3',
        clickhouse_conn_id=CH_CONN_ID,
        sql="""
            INSERT INTO trips
            SELECT * FROM s3(
                '{{ task_instance.xcom_pull(task_ids="get_path") }}',
                '{{ conn.MINIO.login }}', 
                '{{ conn.MINIO.password }}', 
                'CSVWithNames'
            )
        """,
        params={'path': s3_path}
    )
    sensor >> create_table >> load_from_s3




    #exec_date = get_date()
    #data = extract(list_keys_source.output, exec_date)
    #extract_root = unzip(data)
    #load_task = load(extract_root, list_keys_target.output, exec_date)
    #load_task >> cleanup(data, extract_root) >> marker

s3_dag()