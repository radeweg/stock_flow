from datetime import datetime
import logging
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


jars = "/opt/airflow/dags/postgresql-42.5.4.jar"
driver_class_path = "/opt/airflow/dags/postgresql-42.5.4.jar"
spark_job = "/opt/airflow/dags/send_raw_data_to_hdfs.py"
destination_table_name = "long-justice-346420.stock_flow_dataset.stock_flow"
source_bucket = 'databricks-2864737403744337'


query = """SELECT * FROM stock_flow"""


postgres_conn_id = 'postgres_local'
gcp_conn_id = 'google_connection'


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
        driver_class_path=driver_class_path,
        jars=jars,
        dag=dag
    )
    # t2_task_id = f"send_data_to_gcs"
    # store_data_in_gcs = PostgresToGCSOperator(
    #     task_id=t2_task_id,
    #     postgres_conn_id=postgres_conn_id,
    #     gcp_conn_id=gcp_conn_id,
    #     export_format='NEWLINE_DELIMITED_JSON',
    #     bucket='databricks-2864737403744337',
    #     filename='stock_flow_file.json',
    #     sql=query
    # )
    #
    # t3_task_id = f"store_data_in_table"
    # gcs_to_bq = GCSToBigQueryOperator(
    #     task_id=t3_task_id,
    #     gcp_conn_id=gcp_conn_id,
    #     bucket=source_bucket,
    #     source_objects='stock_flow_file.json',
    #     write_disposition="WRITE_TRUNCATE",
    #     source_format="NEWLINE_DELIMITED_JSON",
    #     autodetect=True,
    #     destination_project_dataset_table=destination_table_name
    # )

    start_dag >> get_data_ >> end_dag
