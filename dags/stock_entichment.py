from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.contrib.operators.ssh_operator import SSHOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 27),
}

# Define the DAG
with DAG('stock_enrichment_process', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    get_last_cut_date = SSHOperator(
        task_id='get_last_cut_date',
        ssh_conn_id='ssh_default',  # Define your SSH connection in Airflow
        command='/bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/common/get_last_cut_dates.py | tail -n 1',
        do_xcom_push=True,  # Push stdout to XCom
    )

    # Command to start the stock news process
    stock_news_ninety_days_before_bash = " /bin/python3 /home/developer/projects/spark-course-python/final_project_naya_cde/SPARK_MODULE/API_TO_S3/stock_data/stock_metadata_enrichment.py {{task_instance.xcom_pull(task_ids='get_last_cut_date') }}"
    stock_news_ninety_days_before = SSHOperator(
        ssh_conn_id='ssh_default',
        task_id='get_Stock_enrichment_1',
        command=stock_news_ninety_days_before_bash,
        do_xcom_push=False,
    )

    
    trigger_stock_news_dag = TriggerDagRunOperator(
        task_id='trigger_stock_enrichment_process_1',
        trigger_dag_id='stock_enrichment_process2',
        do_xcom_push=False,
    )

    # Set task dependencies
    get_last_cut_date >> stock_news_ninety_days_before >> trigger_stock_news_dag
