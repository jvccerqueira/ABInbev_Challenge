import os
import requests
import json
from airflow import DAG
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

load_dotenv()

api_url = os.getenv('API_URL')
data_lake = os.getenv('DATA_LAKE')
docker_url = os.getenv('DOCKER_URL')

def retrieve_raw_json(ti, url, data_lake):
    print('Retrieving data from API')
    
    raw_path = f"{data_lake}/bronze/raw"

    response = requests.get(url)
    data = response.json()
    file_name = f"breweries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    file_path = f"{raw_path}/{file_name}"
    with open(file_path, "w") as file:
        file.write(str(data))
    
    ti.xcom_push(key="file_name", value=file_name)
    return file_name

def check_json_quality(ti, data_lake):
    print('Checking JSON quality')

    file_name = ti.xcom_pull(task_ids="retrieve_raw_json", key="file_name")

    file_path = f"{data_lake}/bronze/raw/{file_name}"
    try:
        with open(file_path, 'r') as f:
            json.load(f)
        return "move_json_file"
    except json.JSONDecodeError:
        return "process_json"

def move_json_file(ti, data_lake):
    file_name = ti.xcom_pull(task_ids="retrieve_raw_json", key="file_name")
    raw_path = f"{data_lake}/bronze/raw/{file_name}"
    fixed_path = f"{data_lake}/bronze/fixed/fixed_{file_name}"
    os.system(f'cp "{raw_path}" "{fixed_path}"')

    ti.xcom_push(key="fixed", value=f"fixed_{file_name}")
    return file_name

def process_json(ti, data_lake):
    print('Processing JSON file')

    filename = ti.xcom_pull(task_ids="retrieve_raw_json", key="file_name")
    file_path = f"{data_lake}/bronze/raw/{filename}"
    output_file = f"{data_lake}/bronze/fixed/fixed_{filename}"

    with open(file_path, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            fixed_line = line.replace("'", '"').replace(" None", " null")
            outfile.write(fixed_line)
    
    ti.xcom_push(key="fixed", value=f'fixed_{filename}')
    return f'fixed_{filename}'

def notificator(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    print(f"DAG {dag_id} Ended with errors")
    print(f"Task '{task_id}")
    print(f"Execution Date: {execution_date}")
    print(f"Logs: {log_url}")
    print(f"Sending email to {os.getenv('EMAIL')}")


with DAG(
    dag_id="Brewery_Pipeline",
    start_date=datetime(2021, 1, 1),
    schedule='@daily',
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=5),
        'on_failure_callback': notificator
    },
    doc_md=__doc__,
    catchup=False
):
    start = EmptyOperator(task_id="start")

    retrieve_raw_json = PythonOperator(
        task_id="retrieve_raw_json",
        python_callable=retrieve_raw_json,
        op_kwargs={"url": api_url, "data_lake": data_lake}
    )

    wait_file = FileSensor(
        task_id="wait_file",
        filepath=f"{data_lake}/bronze/raw/*.json",
        poke_interval=5,
        timeout=200,
        mode="poke"
    )

    check_json_quality = BranchPythonOperator(
        task_id="check_json_quality",
        python_callable=check_json_quality,
        op_kwargs={"data_lake": data_lake}
    )

    move_json_file = PythonOperator(
        task_id="move_json_file",
        python_callable=move_json_file,
        op_kwargs={"data_lake": data_lake}
    )

    process_json = PythonOperator(
        task_id="process_json",
        python_callable=process_json,
        op_kwargs={"data_lake": data_lake}
    )

    gen_parquet_partition_by_location = DockerOperator(
        task_id="gen_parquet_partition_by_location",
        image="silver-processing:latest",
        api_version="auto",
        auto_remove="success",
        command=["/opt/spark/bin/spark-submit", "silver_processing.py"],
        docker_url=docker_url,
        mounts=[Mount(source=data_lake, target="/data_lake", type="bind")],
        trigger_rule="one_success"
    )

    gen_view_by_brewery_and_location = DockerOperator(
        task_id="gen_view_by_brewery_and_location",
        image="gold-processing:latest",
        api_version="auto",
        auto_remove="success",
        command=["/opt/spark/bin/spark-submit", "gold_processing.py"],
        docker_url=docker_url,
        mounts=[Mount(source=data_lake, target="/data_lake", type="bind")],
    )

    end = EmptyOperator(task_id="end")

    start >> retrieve_raw_json >> wait_file >> check_json_quality >> [process_json, move_json_file] >> gen_parquet_partition_by_location >> gen_view_by_brewery_and_location >> end
