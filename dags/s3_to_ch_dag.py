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
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator

default_args = {
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

with DAG(
        's3_to_clickhouse_trips',
        default_args=default_args,
        schedule_interval='@monthly',  # или по завершении первого DAG
        catchup=False
) as dag:
    # Определяем переменные для пути (например, для декабря 2024)
    # В реальном DAG используйте {{ ds_format(ds, '%Y-%m-%d', '%Y%m') }}
    s3_path = "s3://your-bucket/202412/*.csv"

    # 1. Создаем таблицу (если нет)
    create_table = ClickHouseOperator(
        task_id='create_table',
        sql="""
        CREATE TABLE IF NOT EXISTS trips (
            ride_id String,
            rideable_type String,
            started_at DateTime,
            ended_at DateTime,
            -- добавьте остальные поля согласно структуре CSV
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(started_at)
        ORDER BY started_at;
        """
    )

    # 2. Загружаем данные напрямую из S3
    # Используем функцию s3(), где указываем путь, формат и структуру
    load_from_s3 = ClickHouseOperator(
        task_id='load_from_s3',
        sql=f"""
        INSERT INTO trips
        SELECT * FROM s3(
            '{s3_path}',
            'AWS_ACCESS_KEY_ID', 
            'AWS_SECRET_ACCESS_KEY', 
            'CSVWithNames'
        )
        """
    )

    create_table >> load_from_s3


BUCKET_NAME_TARGET = "bucket"

AWS_CONN_ID_TARGET = "MINIO"

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

    list_keys_source = S3ListOperator(
        task_id="list_keys_target",
        bucket=BUCKET_NAME_TARGET,
        aws_conn_id=AWS_CONN_ID_TARGET,
    )



    exec_date = get_date()
    data = extract(list_keys_source.output, exec_date)
    extract_root = unzip(data)
    load_task = load(extract_root, list_keys_target.output, exec_date)
    load_task >> cleanup(data, extract_root) >> marker

s3_dag()