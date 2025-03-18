# %%
import os
import requests
from dotenv import load_dotenv
from datetime import datetime

# %%
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
    
def process_json(filename, data_lake):
    print('Processing JSON file')

    file_path = f"{data_lake}/bronze/raw/{filename}"
    output_file = f"{data_lake}/bronze/fixed/fixed_{filename}"

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

    file_name  = retrieve_raw_json(api_url, data_lake)
    process_json(file_name, data_lake)