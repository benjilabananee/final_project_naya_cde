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
with DAG('stock_news', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    stock_news_ninety_days_before_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API/api_get_stock_news.py"""
    # stock_news_ninety_days_before_bash = ("pwd")
    stock_news_ninety_days_before = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='get_stock_news',
        command=stock_news_ninety_days_before_bash,
    )
    
    stock_news_ninety_days_before
     
