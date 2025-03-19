import os
from pyspark.sql import SparkSession

def gen_parquet_partition_by_location(fixed, data_lake):
    spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()

    fixed_path = f'{data_lake}/bronze/fixed'
    silver_path = f'{data_lake}/silver'
    processed_path = f'{data_lake}/bronze/processed'
    location = 'state'

    df = spark.read.option("multiLine", "true").json(f'{fixed_path}/{fixed}')

    # Saving processed data in parque files
    file_path = f"{silver_path}/breweries"
    df.write.partitionBy(location).mode("overwrite").parquet(file_path)

    # Moving processed files to processed folder
    os.rename(os.path.join(fixed_path, fixed), os.path.join(processed_path, fixed))

    # os.chmod(f"{silver_path}/breweries", 0o777)
    # os.chmod(os.path.join(processed_path, fixed), 0o777)

if __name__ == "__main__":
    print("Starting Silver Processing on Docker")
    data_lake_path = os.getenv("DATA_LAKE", "/data_lake")
    fixed = os.listdir(f"{data_lake_path}/bronze/fixed")[0]
    gen_parquet_partition_by_location(fixed, data_lake_path)