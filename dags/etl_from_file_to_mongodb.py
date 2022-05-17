# airflow packeges
from airflow import DAG
from airflow.operators.python import PythonOperator

# others packeges
from datetime import timedelta, datetime
import pandas as pd
import xlwings as xw


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class ExtractDataFromXls:
  def __init__(self) -> None:
      pass

  def check_transient_zone():
    pass

  def check_raw_zone():
    pass

  def check_trusted_zone():
    pass

  def etl_from_transient_to_raw_zone():
    pass

  def etl_from_raw_to_trusted_zone():
    pass


  # def teste(file_path, file_name):

  #     with xw.App(visible=False) as app:
  #     book = xw.Book(file_path.format(file_name))
  #     book.sheets[0]["C49"].value = "Amazonas"

  #     book.save(file_path.format(file_name))
  #     book.close()
      

  #     df = pd.read_excel(
  #         file_path.format(file_name),
  #         sheet_name='Plan1',
  #         usecols='B:W',
  #         skiprows=52,
  #         nrows=12
  #     )

with DAG(
    default_args = {
        "owner": "Diego Lopes",
        "depends_on_past": False,
        "start_date": datetime(2022, 6, 23),
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "provide_context": True
    },
    dag_id="Extract data from XLS",
    description="Responsible to extract data from XLS files, transform them and save as parquet file",
    schedule_interval="30 4 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    ObjEtl = ExtractDataFromXls()

    check_transient_zone = PythonOperator(
        task_id="teste",
        python_callable=ObjEtl.check_transient_zone,
        dag=dag
    )

    check_raw_zone = PythonOperator(
        task_id="teste",
        python_callable=ObjEtl.check_raw_zone,
        dag=dag
    )

    check_trusted_zone = PythonOperator(
        task_id="teste",
        python_callable=ObjEtl.check_trusted_zone,
        dag=dag
    )

    etl_from_transient_to_raw_zone = PythonOperator(
        task_id="teste",
        python_callable=ObjEtl.etl_from_transient_to_raw_zone,
        dag=dag
    )

    etl_from_raw_to_trusted_zone = PythonOperator(
        task_id="teste",
        python_callable=ObjEtl.etl_from_raw_to_trusted_zone,
        dag=dag
    )


    [check_transient_zone, check_raw_zone, check_trusted_zone] > etl_from_transient_to_raw_zone > etl_from_raw_to_trusted_zone