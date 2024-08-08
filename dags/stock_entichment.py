from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 27)
}

# Define the DAG
with DAG('stock_enrichment_process', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # Command to kill any existing stock news process
    kill_process_bash = "ps aux | grep '[s]tock_metadata_enrichment.py' | awk '{print $2}' | xargs -r kill -15"
    kill_process = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='kill_existing_process',
        command=kill_process_bash,
    )


    # Command to start the stock news process
    stock_news_ninety_days_before_bash = "/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API_TO_S3/stock_data/stock_metadata_enrichment.py"
    stock_news_ninety_days_before = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='stock_enrichment',
        command=stock_news_ninety_days_before_bash,
    )

    # Set task dependencies
    kill_process >> stock_news_ninety_days_before
