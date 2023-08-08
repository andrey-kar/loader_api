from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from loader_api import loader


api_loader_dag = DAG('api_loader_dag',
                     schedule_interval="0 12,00 * * *",
                     start_date=days_ago(0, 0, 0, 0, 0)
                     )

download_to_db = PythonOperator(
    task_id='download_to_db',
    python_callable=loader.main,
    dag=api_loader_dag
    )
