#!/bin/bash

# Need to have java jdk installed
# Need to have python3 installed

ENV_FILE=".env"

echo "API_URL='https://api.openbrewerydb.org/breweries'" > $ENV_FILE
echo "DATA_LAKE=$(pwd)/data_lake" >> $ENV_FILE

python3 -m venv .venv
source .venv/bin/activate

AIRFLOW_VERSION=2.10.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install pyspark requests python-dotenv

export AIRFLOW_HOME=$(pwd)
# airflow standalone