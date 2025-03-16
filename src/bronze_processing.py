# %%
import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

# %%
@retry(
    stop=stop_after_attempt(5),  # Máximo de 5 tentativas
    wait=wait_exponential(multiplier=1, min=1, max=16),  # Espera exponencial (1s, 2s, 4s, ...)
    retry=(lambda exc: isinstance(exc, requests.Timeout))  # Tenta novamente apenas para Rate Limit Exceeded
)
def retrieve_raw_json(url, bronze_path):
    print('Retrieving data from API')
    response = requests.get(url)
    try:
        if response.status_code == 200:
            data = response.json()
            file_path = f"{bronze_path}/breweries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(file_path, "w") as file:
                file.write(str(data))
            return "Success"
    except requests.HTTPError as e:
        return e
# %%
if __name__ == "__main__":
    print("Running bronze_processing.py")
    load_dotenv()

    bronze_dir = os.getenv('BRONZE_DIR')
    api_url = os.getenv('API_URL')
    retrieve_raw_json(api_url, bronze_dir)

# %%
