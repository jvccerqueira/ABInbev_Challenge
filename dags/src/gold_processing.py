# %%
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# %%
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

if __name__ == "__main__":
    gold_processing()