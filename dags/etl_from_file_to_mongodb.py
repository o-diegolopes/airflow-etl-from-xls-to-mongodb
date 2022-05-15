# airflow related

from airflow import DAG
from airflow.operators.python import PythonOperator

# others packeges
from datetime import timedelta, datetime


def teste():
    pass


with DAG(
    default_args = {
        "owner": "Diego Lopes",
        "depends_on_past": False,
        "start_date": datetime(2022, 6, 23),
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "provide_context": True
    },
    dag_id="teste_dag",
    description="Responsible for obtaining new customers from sorocaba database and enriching them with API data",
    schedule_interval="30 4 * * *",
    catchup=False,
    max_active_runs=1
) as dag:


    create_temp_path = PythonOperator(
        task_id="teste",
        python_callable=teste,
        dag=dag
    )


    create_temp_path