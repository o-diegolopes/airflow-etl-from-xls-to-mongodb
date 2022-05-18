# airflow packeges
from airflow import DAG
from airflow.operators.python import PythonOperator

# others packeges
from datetime import timedelta, datetime
from numpy import double
import pandas as pd
import xlwings as xw
import os, locale, shutil


locale.setlocale(locale.LC_ALL, 'pt_BR')


class ExtractDataFromXls:
  def __init__(self) -> None:
    self.__project_path = "/Users/diegolopes/Desktop/repositories/airflow-etl-from-xls-to-mongodb"
    self.transient_zone_path = f"{self.__project_path}/data-lake/transient-zone"
    self.raw_zone_path = f"{self.__project_path}/data-lake/raw-zone"
    self.trusted_zone_path = f"{self.__project_path}/data-lake/trusted-zone"
    self.storage = f"{self.__project_path}/storage"
    self.xls_file = "vendas-combustiveis-m3.xls"
    self.federation_units = {
      "AC": "Acre",
      "AL": "Alagoas",
      "AP": "Amapá",
      "AM": "Amazonas",
      "BA": "Bahia",
      "CE": "Ceara",
      "DF": "Distrito Federal",
      "ES": "Espírito Saanto",
      "GO": "Goiás",
      "MA": "Maranhão",
      "MT": "Mato Grosso",
      "MS": "Mato Grosso do Sul",
      "MG": "Minas Gerais",
      "PA": "Pará",
      "PB": "Paraíba",
      "PR": "Paraná",
      "PE": "Pernambuco",
      "PI": "Piauí",
      "RJ": "Rio de Janeiro",
      "RN": "Rio Grande do Norte",
      "RS": "Rio Grande do Sul",
      "RO": "Rondônia",
      "RR": "Roraima",
      "SC": "Santa Catarina",
      "SP": "São Paulo",
      "SE": "Sergipe",
      "TO": "Tocantins"
    }
    self.oil_derivative_fuels = [
      "ETANOL HIDRATADO (m3)",
      "GASOLINA C (m3)",
      "GASOLINA DE AVIAÇÃO (m3)",
      "GLP (m3)",
      "ÓLEO COMBUSTÍVEL (m3)",
      "ÓLEO DIESEL (m3)",
      "QUEROSENE DE AVIAÇÃO (m3)",
      "QUEROSENE ILUMINANTE (m3)"
    ]
    self.diesel_types = [
      "ÓLEO DIESEL (OUTROS ) (m3)",
      "ÓLEO DIESEL MARÍTIMO (m3)",
      "ÓLEO DIESEL S-10 (m3)",
      "ÓLEO DIESEL S-1800 (m3)",
      "ÓLEO DIESEL S-500 (m3)"
    ]
    self.oil_derivative_fuels_table = {
      "usecols": "B:W",
      "skiprows": 52,
      "nrows": 12,
      "sheet": "Plan1"
    }
    self.diesel_table = {
      "usecols": "B:J",
      "skiprows": 132,
      "nrows": 12,
      "sheet": "Plan1"
    }


  def check_if_exists_transient_zone(self, *args, **kwargs):
    if not os.path.exists(self.transient_zone_path):
      os.makedirs(self.transient_zone_path)

  def check_if_exists_raw_zone(self, *args, **kwargs):
    if not os.path.exists(self.raw_zone_path):
      os.makedirs(self.raw_zone_path)

  def check_if_exists_trusted_zone(self, *args, **kwargs):
    if not os.path.exists(self.trusted_zone_path):
      os.makedirs(self.trusted_zone_path)

  def copy_data_from_storage_to_transient_zone(self, *args, **kwargs):
    """"
    This function make a copy of files in storage to transiente zone.
    """
    shutil.copyfile(f"{self.storage}/{self.xls_file}", f"{self.transient_zone_path}/{self.xls_file}")

  def extract_oil_derivative_fuels_data_to_raw_zone(self, *args, **kwargs):
    """"
    This function extract oil derivative fuels data from XLS in transient zone to raw zone.
    """
    frames = []
    for key, units in self.federation_units.items():
      for product in self.oil_derivative_fuels:

        with xw.App(visible=False) as app:
          book = xw.Book(f"{self.transient_zone_path}/{self.xls_file}")
          book.sheets[0]["C49"].value = units
          book.sheets[0]["C50"].value = product

          book.save(f"{self.transient_zone_path}/{self.xls_file}")
          book.close()

          df = pd.read_excel(
              f"{self.storage}/{self.xls_file}",
              sheet_name=self.oil_derivative_fuels_table.get("sheet"),
              usecols=self.oil_derivative_fuels_table.get("usecols"),
              skiprows=self.oil_derivative_fuels_table.get("skiprows"),
              nrows=self.oil_derivative_fuels_table.get("nrows")
          )

          get_month = lambda x: datetime.strptime(x, '%B').month

          df = df.set_index('Dados').stack().reset_index()
          df = df.rename(columns={0: "volume"})
          df["product"] = product[:-5]
          df["uf"] = key
          df["unit"] = product[-3:-1]
          df["year_month"] = df.apply(
            lambda row: f"{str(row['level_1'])}-{str(get_month(row['Dados']) if get_month(row['Dados']) >= 10 else '0'+ str(get_month(row['Dados'])))}", axis=1
          )
          df["volume"] = df.apply(lambda row: round(float(row['volume']), 3), axis=1)
          df["create_at"] = datetime.today.strftime('%Y-%m-%d')
          
          df = df.drop(["Dados", "level_1"],  axis=1).set_index('year_month').reset_index()

          frames.append(df)
          
    dataframe = pd.concat(frames)
    dataframe.to_csv(f"{self.raw_zone_path}/oil_derivative_fuels.csv")

  def extract_diesel_fuel_data_to_raw_zone(self, *args, **kwargs):
    """"
    This function extract diesel fuel data from XLS in transient zone to raw zone.
    """
    frames = []
    for key, units in self.federation_units.items():
      for product in self.diesel_types:

        with xw.App(visible=False) as app:
          book = xw.Book(f"{self.transient_zone_path}/{self.xls_file}")
          book.sheets[0]["C129"].value = units
          book.sheets[0]["C130"].value = product

          book.save(f"{self.transient_zone_path}/{self.xls_file}")
          book.close()

          df = pd.read_excel(
              f"{self.storage}/{self.xls_file}",
              sheet_name=self.diesel_table.get("sheet"),
              usecols=self.diesel_table.get("usecols"),
              skiprows=self.diesel_table.get("skiprows"),
              nrows=self.diesel_table.get("nrows")
          )

          get_month = lambda x: datetime.strptime(x, '%B').month

          df = df.set_index('Dados').stack().reset_index()
          df = df.rename(columns={0: "volume"})
          df["product"] = product[:-5]
          df["uf"] = key
          df["unit"] = product[-3:-1]
          df["year_month"] = df.apply(
            lambda row: f"{str(row['level_1'])}-{str(get_month(row['Dados']) if get_month(row['Dados']) >= 10 else '0'+ str(get_month(row['Dados'])))}", axis=1
          )
          df["volume"] = df.apply(lambda row: round(float(row['volume']), 3), axis=1)
          df["create_at"] = datetime.today().strftime('%Y-%m-%d')
          
          df = df.drop(["Dados", "level_1"],  axis=1).set_index('year_month').reset_index()

          frames.append(df)
          
    dataframe = pd.concat(frames)
    dataframe.to_csv(f"{self.raw_zone_path}/diesel_fuels.csv")
  
  
  
  def load_data_to_mongodb(*args, **kwargs):
    pass

  def clear_transient_zone(self, *args, **kwargs):
    shutil.rmtree(f"{self.transient_zone_path}")

etl = ExtractDataFromXls()

etl.clear_transient_zone()
etl.check_if_exists_transient_zone()
etl.copy_data_from_storage_to_transient_zone()
etl.extract_diesel_fuel_data_to_raw_zone()

# dag = DAG(
#     default_args = {
#         "owner": "Diego Lopes",
#         "depends_on_past": False,
#         "start_date": datetime(2022, 5, 17),
#         "retries": 3,
#         "retry_delay": timedelta(minutes=3),
#         "provide_context": True
#     },
#     dag_id="extract_data_from_xls",
#     description="Responsible to extract data from XLS files, transform them and save as parquet file",
#     schedule_interval="05 15 * * *",
#     catchup=False,
#     max_active_runs=1
# )

# ObjEtl = ExtractDataFromXls()

# check_if_exists_transient_zone = PythonOperator(
#     task_id="check_if_exists_transient_zone",
#     python_callable=ObjEtl.check_if_exists_transient_zone,
#     dag=dag
# )

# check_if_exists_raw_zone = PythonOperator(
#     task_id="check_if_exists_raw_zone",
#     python_callable=ObjEtl.check_if_exists_raw_zone,
#     dag=dag
# )

# check_if_exists_trusted_zone = PythonOperator(
#     task_id="check_if_exists_trusted_zone",
#     python_callable=ObjEtl.check_if_exists_trusted_zone,
#     dag=dag
# )


# extract_data_from_xls_to_transient = PythonOperator(
#     task_id="extract_data_from_xls_to_transient",
#     python_callable=ObjEtl.extract_data_from_xls_to_transient,
#     dag=dag
# )

# consolidate_data_and_load_to_raw = PythonOperator(
#     task_id="consolidate_data_and_load_to_raw",
#     python_callable=ObjEtl.consolidate_data_and_load_to_raw,
#     dag=dag
# )

# transform_data_from_raw_to_trusted = PythonOperator(
#     task_id="transform_data_from_raw_to_trusted",
#     python_callable=ObjEtl.transform_data_from_raw_to_trusted,
#     dag=dag
# )

# load_data_to_mongodb = PythonOperator(
#     task_id="load_data_to_mongodb",
#     python_callable=ObjEtl.load_data_to_mongodb,
#     dag=dag
# )

# clear_transient_zone = PythonOperator(
#     task_id="clear_transient_zone",
#     python_callable=ObjEtl.clear_transient_zone,
#     dag=dag
# )

# extract_data_from_xls_to_transient.set_upstream([check_if_exists_transient_zone, check_if_exists_raw_zone, check_if_exists_trusted_zone])
# extract_data_from_xls_to_transient.set_downstream(consolidate_data_and_load_to_raw)
# consolidate_data_and_load_to_raw.set_downstream(transform_data_from_raw_to_trusted)
# transform_data_from_raw_to_trusted.set_downstream(load_data_to_mongodb)
# load_data_to_mongodb.set_downstream(clear_transient_zone)