# %%
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def gen_parquet_partition_by_location(data_lake):
    spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()

    fixed_path = f'{data_lake}/bronze/fixed'
    silver_path = f'{data_lake}/silver'
    processed_path = f'{data_lake}/bronze/processed'
    location = 'state'

    for fixed in os.listdir(fixed_path):
        if fixed.endswith(".json"):
            print(fixed)
            df = spark.read.option("multiLine", "true").json(f'{fixed_path}/{fixed}')

            # Saving processed data in parque files
            file_path = f"{silver_path}/breweries"
            df.write.partitionBy(location).mode("overwrite").parquet(file_path)

            # Moving processed files to processed folder
            original_path = os.path.join(fixed_path, fixed)
            new_path = os.path.join(processed_path, fixed)
            os.rename(original_path, new_path)

# %%
if __name__ == "__main__":
    print("Running silver_processing")
    load_dotenv()

    data_lake = os.getenv("DATA_LAKE")

    gen_parquet_partition_by_location(data_lake)
    
