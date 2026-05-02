FROM apache/airflow:2.10.4

USER airflow
RUN pip install --upgrade pip
USER airflow
RUN pip install --no-cache-dir airflow-clickhouse-plugin