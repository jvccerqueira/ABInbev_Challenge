from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator
from datetime import datetime
from src import bronze_processing


with DAG(
    dag_id="main_dag",
    start_date=datetime(2021, 1, 1),
    schedule='@daily',
    doc_md=__doc__,
):
    start = EmptyOperator(task_id="start")
    bronze_processing = ExternalPythonOperator(
        task_id="bronze_processing",
        python_callable=bronze_processing.retrieve_raw_json,
        op_args=['https://api.openbrewerydb.org/breweries', "C:/Users/joaoc/OneDrive/Área de Trabalho/Projetos/ABInbev/ABInbev_Challenge/data_lake/bronze/raw"],
        python=r"C:\Users\joaoc\OneDrive\Área de Trabalho\Projetos\ABInbev\ABInbev_Challenge\.venv\Scripts\python.exe"
    )
    end = EmptyOperator(task_id="end")

    start >> bronze_processing >> end
