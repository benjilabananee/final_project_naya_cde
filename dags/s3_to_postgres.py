from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.contrib.operators.ssh_operator import SSHOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25),
}

# Define the DAG
with DAG('stock_data', default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    start_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='start',
        command="echo start",
    )
 
    stock_data_from_previous_day_bash ="""/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API/api_get_stock_data.py """
    stock_data_from_previous_day_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='get_stock_data',
        command=stock_data_from_previous_day_bash,
    )
    
    clean_metadata_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API_TO_S3/metadata/metadata_parsing.py"""
    clean_metadata_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='clean_metadata_task',
        command=clean_metadata_bash,
    )

    stock_data_to_postgres_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/S3_TO_POSTGRES/postgres_test.py"""
    stock_data_to_postgres_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='stock_data_to_postgres_task',
        command=stock_data_to_postgres_bash,
    )

    trigger_stock_news_dag = TriggerDagRunOperator(
        task_id='trigger_stock_news_dag',
        trigger_dag_id='stock_news',
    )

    end_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='end',
        command="echo done!!",
    )

    start_task >> stock_data_from_previous_day_task >> clean_metadata_task >> stock_data_to_postgres_task >> trigger_stock_news_dag >> end_task
