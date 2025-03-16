# %%
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, upper

load_dotenv()

bronze_dir = os.getenv("BRONZE_DIR")
silver_dir = os.getenv("SILVER_DIR")
# spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .getOrCreate()
processed_dir = f"{bronze_dir}/processed"
os.makedirs(processed_dir, exist_ok=True)

# %%
files = os.listdir(bronze_dir)
files_to_process = []
for file in files:
    if file.endswith(".json"):
        try:
            text = spark.read.text(f'{bronze_dir}/{file}')
            text = text.withColumn("value", regexp_replace(col("value"), "'", '"'))
            text = text.withColumn("value", regexp_replace(col("value"), "None", 'null'))
            json_data = text.select("value").first()[0]
            fixed_path = f'{bronze_dir}/fixed/fixed_{file}'
            with open(fixed_path, "w") as f:
                f.write(json_data)

            # os.remove(f'{bronze_dir}/{file}')
            original_path = os.path.join(bronze_dir, file)
            new_path = os.path.join(processed_dir, file)
            os.rename(original_path, new_path)
            files_to_process.append(fixed_path)
            break
        except Exception as e:
            print(e)

# %%
for fixed in files_to_process:
    df = spark.read.option("multiLine", "true").json(fixed)
    df = df.withColumn('state', upper(df['state']))
    file_path = f"{silver_dir}/breweries.parquet"
    df.show()
    # df.write.mode("overwrite").parquet(file_path)
    df.coalesce(1).write.mode("overwrite").parquet(file_path)

# %%
df = spark.createDataFrame([(1, "A"), (2, None), (3, "C")], ["id", "value"])
df.write.parquet("output.parquet")

# %%
