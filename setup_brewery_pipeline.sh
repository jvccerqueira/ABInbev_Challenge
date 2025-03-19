#!/bin/bash

# Need to have java jdk installed
# Need to have python3 installed


mkdir -p data_lake/{bronze,bronze/raw,bronze/processed,bronze/fixed,silver,gold}

ENV_FILE=".env"
echo "API_URL='https://api.openbrewerydb.org/breweries'" > $ENV_FILE
echo "DATA_LAKE=$(pwd)/data_lake" >> $ENV_FILE
echo "DOCKER_URL=unix://var/run/docker.sock" >> $ENV_FILE

python3 -m venv .venv
source .venv/bin/activate

AIRFLOW_VERSION=2.10.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[async,postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install requests python-dotenv apache-airflow-providers-docker

docker build -t silver-processing:latest ./dags/src/silver_process/

docker build -t gold-processing:latest ./dags/src/gold_process/

docker context use default

chmod -R 777 .

export AIRFLOW_HOME=$(pwd)
airflow standalone
# airflow dags trigger Brewery_Pipeline
