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

    get_last_cut_date = SSHOperator(
        task_id='get_last_cut_date',
        ssh_conn_id='ssh_default',  # Define your SSH connection in Airflow
        command='/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/common/get_last_cut_dates.py | tail -n 1',
        do_xcom_push=True,  # Push stdout to XCom
    )
 
    stock_data_from_previous_day_bash ="""/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API/api_get_stock_data.py {{task_instance.xcom_pull(task_ids='get_last_cut_date') }}"""
    stock_data_from_previous_day_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='get_stock_data',
        command=stock_data_from_previous_day_bash,
        do_xcom_push=False,
    )
    
    clean_metadata_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API_TO_S3/metadata/metadata_parsing.py"""
    clean_metadata_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='clean_metadata_task',
        command=clean_metadata_bash,
        do_xcom_push=False,
    )

    stock_data_to_postgres_bash = """/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/S3_TO_POSTGRES/postgres_test.py"""
    stock_data_to_postgres_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='stock_data_to_postgres_task',
        command=stock_data_to_postgres_bash,
        do_xcom_push=False,
    )

    trigger_stock_news_dag = TriggerDagRunOperator(
        task_id='trigger_stock_news_dag',
        trigger_dag_id='stock_news',
        do_xcom_push=False,
    )

    end_task = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='end',
        command="echo done!!",
        do_xcom_push=False,
    )

    get_last_cut_date >> stock_data_from_previous_day_task >> clean_metadata_task >> stock_data_to_postgres_task >> trigger_stock_news_dag >> end_task
