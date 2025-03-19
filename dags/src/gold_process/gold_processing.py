from pyspark.sql import SparkSession
import os

def gen_view_by_brewery_and_location(data_lake):
    spark = SparkSession.builder.appName("Gold Processing").getOrCreate()

    parquet_path = f'{data_lake}/silver/breweries'
    gold_path = f'{data_lake}/gold'
    
    os.makedirs(f'{data_lake}/silver', exist_ok=True)
    os.makedirs(f'{data_lake}/gold', exist_ok=True)

    df = spark.read.parquet(parquet_path)
    selected_df = df.select("state", "brewery_type")
    selected_df = selected_df.groupBy("state", "brewery_type").count().orderBy("state", "brewery_type")
    selected_df.write.mode("overwrite").parquet(f"{gold_path}/view_by_state_brewery_type")
    os.chmod(f"{gold_path}/view_by_state_brewery_type", 0o777)

if __name__ == "__main__":
    print("Starting Gold Processing on Docker")
    data_lake_path = os.getenv("DATA_LAKE", "/data_lake")
    gen_view_by_brewery_and_location(data_lake_path)
