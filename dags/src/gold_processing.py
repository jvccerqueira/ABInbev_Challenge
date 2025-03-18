# %%
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# %%
def gen_view_by_brewery_and_location(data_lake):
    spark = SparkSession.builder.appName("Gold Processing").getOrCreate()

    parquet_path = f'{data_lake}/silver/breweries'
    gold_path = f'{data_lake}/gold'

    df = spark.read.parquet(parquet_path)
    selected_df = df.select("state", "brewery_type")
    selected_df = selected_df.groupBy("state", "brewery_type").count().orderBy("state", "brewery_type")
    selected_df.write.mode("overwrite").parquet(f"{gold_path}/view_by_state_brewery_type")

if __name__ == "__main__":
    load_dotenv()

    data_lake = os.getenv("DATA_LAKE")

    gen_view_by_brewery_and_location(data_lake)