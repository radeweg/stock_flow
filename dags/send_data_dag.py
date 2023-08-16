from datetime import datetime
import logging
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_job = "/opt/airflow/dags/send_raw_data_to_hdfs.py"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 12),
    'end_date': datetime(2025, 4, 25),
    'retries': 0,
    'depends_on_past': False,
}

with DAG(
        dag_id="stock_flow",
        schedule_interval="@once",
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=['stock_flow']
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")
    end_dag = EmptyOperator(task_id="end_dag")

    task_id = f"spark_send_raw_data_to_HDFS"
    get_data_ = SparkSubmitOperator(
        task_id=task_id,
        application=spark_job,
        conn_id='spark_conn_id',
        dag=dag
    )

    start_dag >> get_data_ >> end_dag
