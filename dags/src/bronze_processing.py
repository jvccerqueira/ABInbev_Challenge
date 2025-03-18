# %%
import os
import requests
from dotenv import load_dotenv
from datetime import datetime

# %%
def retrieve_raw_json(url, raw_path):
    print('Retrieving data from API')

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

# %%
if __name__ == "__main__":
    print("Running bronze_processing.py")
    load_dotenv()
    api_url = os.getenv('API_URL')
    data_lake = os.getenv('DATA_LAKE')

    bronze_dir = f'{data_lake}/bronze'
    os.makedirs(bronze_dir, exist_ok=True)
    raw_path = f"{bronze_dir}/raw"
    os.makedirs(raw_path, exist_ok=True)
    fixed_path = f"{bronze_dir}/fixed"
    os.makedirs(fixed_path, exist_ok=True)

    file_name  = retrieve_raw_json(api_url, raw_path)
    process_json(file_name, raw_path, fixed_path)