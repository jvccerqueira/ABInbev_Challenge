import os
import requests
from dotenv import load_dotenv
from datetime import datetime
# from pyspark.sql import SparkSession
from airflow.models.dag import DAG
from airflow.operators.python import PythonVirtualenvOperator

def retrieve_raw_json(url, data_lake):
    print('Retrieving data from API')
    raw_path = f"{data_lake}/bronze/raw"

    response = requests.get(url)
    try:
        data = response.json()
        file_name = f"breweries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = f"{raw_path}/{file_name}"
        with open(file_path, "w") as file:
            file.write(str(data))
        return file_name
    except requests.HTTPError as e:
        return e
    
def process_json(filename, raw, fixed_path):
    print('Processing JSON file')
    file_path = f"{raw}/{filename}"
    output_file = f"{fixed_path}/fixed_{filename}"
    with open(file_path, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            fixed_line = line.replace("'", '"').replace(" None", " null")
            outfile.write(fixed_line)

def silver_layer_processing():
    load_dotenv()
    spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()

    data_lake = os.getenv("DATA_LAKE")
    bronze_path = f'{data_lake}/bronze'
    os.makedirs(bronze_path, exist_ok=True)
    silver_path = f'{data_lake}/silver'
    os.makedirs(silver_path, exist_ok=True)

    fixed_path = f'{bronze_path}/fixed'
    os.makedirs(fixed_path, exist_ok=True)
    processed_path = f'{bronze_path}/processed'
    os.makedirs(processed_path, exist_ok=True)

    for fixed in os.listdir(fixed_path):
        if fixed.endswith(".json"):
            print(fixed)
            df = spark.read.option("multiLine", "true").json(f'{fixed_path}/{fixed}')

            # Saving processed data in parque files
            file_path = f"{silver_path}/breweries"
            df.write.partitionBy('state').mode("overwrite").parquet(file_path)

            # Moving processed files to processed folder
            original_path = os.path.join(fixed_path, fixed)
            new_path = os.path.join(processed_path, fixed)
            os.rename(original_path, new_path)

def gold_processing():
    load_dotenv()
    spark = SparkSession.builder.appName("Gold Processing").getOrCreate()

    data_lake = os.getenv("DATA_LAKE")
    silver_path = f'{data_lake}/silver'
    gold_path = f'{data_lake}/gold'

    # %%
    parquet_files = os.listdir(silver_path)
    for file in parquet_files:
        df = spark.read.parquet(f"{silver_path}/{file}")
        selected_df = df.select("state", "brewery_type")
        selected_df = selected_df.groupBy("state", "brewery_type").count().orderBy("state", "brewery_type")
        selected_df.write.parquet(f"{gold_path}/view_by_state_brewery_type")

#Setting up the Airflow DAG to run the pipeline
with DAG(
    "brewery_pipeline", 
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily"
    ) as dag:
    bronze_process = PythonVirtualenvOperator(
        task_id="bronze_process",
        python_callable=retrieve_raw_json,
        op_kwargs={
            "url": "https://api.openbrewerydb.org/breweries",
            "data_lake": os.getenv("DATA_LAKE")
        }
    )
