from datetime import datetime
import logging
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

jars = "/opt/airflow/dags/postgresql-42.5.4.jar"
driver_class_path = "/opt/airflow/dags/postgresql-42.5.4.jar"
spark_job = "/opt/airflow/dags/main.py"


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

    task_id = f"run_spark_job"
    get_data_ = SparkSubmitOperator(
        task_id=task_id,
        application=spark_job,
        conn_id='spark_conn_id',
        driver_class_path=driver_class_path,
        jars=jars,
        dag=dag
    )

    start_dag >> get_data_ >> end_dag
