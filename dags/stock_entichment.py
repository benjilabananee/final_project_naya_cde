from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.contrib.operators.ssh_operator import SSHOperator # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 27),
}

# Define the DAG
with DAG('stream_stock_to_s3_after_enrichment', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    stock_after_enrichment_to_s3_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API_TO_S3/stock_data/stock_metadata_enrichment.py"""
    stock_after_enrichment_to_s3 = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='send_stock_after_enrichment_to_s3',
        command=stock_after_enrichment_to_s3_bash,
    )
    
    stock_after_enrichment_to_s3
