# %%
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper


def silver_layer_processing():
    load_dotenv()
    spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()

    bronze_path = os.getenv("BRONZE_DIR")
    os.makedirs(bronze_path, exist_ok=True)
    silver_path = os.getenv("SILVER_DIR")
    os.makedirs(silver_path, exist_ok=True)

    fixed_path = f'{bronze_path}/fixed'
    os.makedirs(fixed_path, exist_ok=True)
    processed_path = f'{bronze_path}/processed'
    os.makedirs(processed_path, exist_ok=True)

    for fixed in os.listdir(fixed_path):
        if fixed.endswith(".json"):
            print(fixed)
            df = spark.read.option("multiLine", "true").json(f'{fixed_path}/{fixed}')
            df = df.withColumn('state', upper(df['state']))

            # Saving processed data in parque files
            file_path = f"{silver_path}/breweries"
            df.write.partitionBy('state').mode("overwrite").parquet(file_path)

            # Moving processed files to processed folder
            original_path = os.path.join(fixed_path, fixed)
            new_path = os.path.join(processed_path, fixed)
            os.rename(original_path, new_path)

# %%
if __name__ == "__main__":
    print("Running silver_processing")
    silver_layer_processing()
    
