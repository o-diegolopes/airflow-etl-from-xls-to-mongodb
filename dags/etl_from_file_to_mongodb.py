# airflow packeges
from airflow import DAG
from airflow.operators.python import PythonOperator

# others packeges
from datetime import timedelta, datetime
import pandas as pd
import xlwings as xw
import os


# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)


class ExtractDataFromXls:
  def __init__(self) -> None:
    self.__project_path = "/Users/diegolopes/Desktop/repositories/airflow-etl-from-xls-to-mongodb"
    self.transient_zone_path = f"{self.__project_path}/data-lake/transient-zone"
    self.raw_zone_path = f"{self.__project_path}/data-lake/raw-zone"
    self.trusted_zone_path = f"{self.__project_path}/data-lake/trusted-zone"



  def check_if_exists_transient_zone(self, *args, **kwargs):
    if not os.path.exists(self.transient_zone_path):
      os.makedirs(self.transient_zone_path)

  def check_if_exists_raw_zone(self, *args, **kwargs):
    if not os.path.exists(self.raw_zone_path):
      os.makedirs(self.raw_zone_path)

  def check_if_exists_trusted_zone(self, *args, **kwargs):
    if not os.path.exists(self.trusted_zone_path):
      os.makedirs(self.trusted_zone_path)

  def extract_data_from_xls_to_transient(*args, **kwargs):
    pass

  def consolidate_data_and_load_to_raw(*args, **kwargs):
    pass
  
  def transform_data_from_raw_to_trusted(*args, **kwargs):
    pass

  def load_data_to_mongodb(*args, **kwargs):
    pass

  def clear_transient_zone(*args, **kwargs):
    pass

  def teste(file_path, file_name):
    pass

      # with xw.App(visible=False) as app:
      # book = xw.Book(file_path.format(file_name))
      # book.sheets[0]["C49"].value = "Amazonas"

      # book.save(file_path.format(file_name))
      # book.close()

      # df = pd.read_excel(
      #     file_path.format(file_name),
      #     sheet_name='Plan1',
      #     usecols='B:W',
      #     skiprows=52,
      #     nrows=12
      # )

dag = DAG(
    default_args = {
        "owner": "Diego Lopes",
        "depends_on_past": False,
        "start_date": datetime(2022, 6, 23),
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "provide_context": True
    },
    dag_id="extract_data_from_xls",
    description="Responsible to extract data from XLS files, transform them and save as parquet file",
    schedule_interval="30 4 * * *",
    catchup=False,
    max_active_runs=1
)

ObjEtl = ExtractDataFromXls()

check_if_exists_transient_zone = PythonOperator(
    task_id="check_if_exists_transient_zone",
    python_callable=ObjEtl.check_if_exists_transient_zone,
    dag=dag
)

check_if_exists_raw_zone = PythonOperator(
    task_id="check_if_exists_raw_zone",
    python_callable=ObjEtl.check_if_exists_raw_zone,
    dag=dag
)

check_if_exists_trusted_zone = PythonOperator(
    task_id="check_if_exists_trusted_zone",
    python_callable=ObjEtl.check_if_exists_trusted_zone,
    dag=dag
)


extract_data_from_xls_to_transient = PythonOperator(
    task_id="extract_data_from_xls_to_transient",
    python_callable=ObjEtl.extract_data_from_xls_to_transient,
    dag=dag
)

consolidate_data_and_load_to_raw = PythonOperator(
    task_id="consolidate_data_and_load_to_raw",
    python_callable=ObjEtl.consolidate_data_and_load_to_raw,
    dag=dag
)

transform_data_from_raw_to_trusted = PythonOperator(
    task_id="transform_data_from_raw_to_trusted",
    python_callable=ObjEtl.transform_data_from_raw_to_trusted,
    dag=dag
)

load_data_to_mongodb = PythonOperator(
    task_id="load_data_to_mongodb",
    python_callable=ObjEtl.load_data_to_mongodb,
    dag=dag
)

clear_transient_zone = PythonOperator(
    task_id="clear_transient_zone",
    python_callable=ObjEtl.clear_transient_zone,
    dag=dag
)

extract_data_from_xls_to_transient.set_upstream([check_if_exists_transient_zone, check_if_exists_raw_zone, check_if_exists_trusted_zone])
extract_data_from_xls_to_transient.set_downstream(consolidate_data_and_load_to_raw)
consolidate_data_and_load_to_raw.set_downstream(transform_data_from_raw_to_trusted)
transform_data_from_raw_to_trusted.set_downstream(load_data_to_mongodb)
load_data_to_mongodb.set_downstream(clear_transient_zone)