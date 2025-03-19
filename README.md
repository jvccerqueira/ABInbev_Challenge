# Brewery Data Pipeline
This pipeline consumes data from Brewery API and loads this data in a data lake following the medallion architecture.
It was developed following "BEES Data Engineering – Breweries Case" challenge
- **Bronze Layer**: stores and persists raw data
- **Silver Layer**: transform data of bronze layer into a parquet format partitioned by location
- **Gold Layer**: Aggregate the parquet files available on silver layer and aggregate by brewery type and location
All data transformations are developed with PySpark and all its distributed processing features, the pipeline is orchestrated by Apache Airflow.

## Instalation and Execution
>[!NOTE]
>This pipeline was developed with Ubuntu running with WSL in Windows

>[!TIP]
>Searching for Ubuntu on Microsoft Store you can download and access all Ubuntu feature inside windows, for example run a Ubuntu CLI, which will be the only tool we will need to proceed with instalation.

> [!IMPORTANT]
> Before proceed, make sure that you have the following softwares installed on your system
> * Docker
>   * Inside the script `setup_brewery_pipeline.sh` there is the command to create the variable `DOCKER_URL` which pointing to the default target in linux, if your docker is set in another folder, please change this variable to point to your folder
> * Python 3

To configure this pipeline, follow these steps
1. Clone this repository to your machine
2. Change the permissions of the file `setup_brewery_pipeline.sh` to make it executable
   * Inside the folder where the repository was cloned run `chmod +x setup_brewery_pipeline.sh` 
3. Run the script with `./setup_brewery_pipeline.sh`
   - This will create all necessary folder, install depencies needed and execute Airflow 
   - By default Airflow will run a web service on `localhost:8080` once you access it in your browser a user and password will be request

4. Inside Airflow UI you can search for DAG `Brewery_Pipeline` and execute it. This will trigger the process to download and transform brewery data, this can also be triggered via terminal running `airflow dags trigger Brewery_Pipeline`
   
This process will create the folder `data_lake` inside the directory you cloned this repository
- `data_lake`
  - `/bronze`
    - `/raw` persists raw data consumed from API
    - `/fixed` store JSON fixed to be successfully consumed by PySpark
    - `/processed` after fixed files are processed it persists this files
  - `/silver` contains parquet file partitioned by location 
  - `/gold` contains parquet file aggregated by brewery_type and location
## Process Detailing
### setup_brewery_pipeline
The shell script was writen to create, install and execute all necessary steps to trigger to pipeline in Airflow
1. The script will create all data_lake folders
2. Create `.env` file based on current system info
3. Create Python Virtual environment and install and libraries inside it
4. Build container images based on dockerfile's at `dags/src/[gold,silver]`
    * Both gold and silver dockerfiles uses `apache/spark-py` image as base to build our image
5. Changes permissions of files and folders created
6. Finally runs Airflow Standalone which configure Airflow users, permissions and everything that is needed to execute and trigger DAGs as this will be the first time that this Airflow instance is running, as mentioned before the UI will be available under `localhost:8080` url, by default
     * User is `admin` by default
     * Password you can get it on the CLI Lines on the terminal that the setup script was executed or on the file `standalone_admin_password.txt` that is automatically generate on the folder where you cloned this repository
It needs some time to Airflow make our DAG available, so once you run the UI webserver wait a little until the DAG `Brewery_Pipeline` appears in the list

### DAG Brewery_Pipeline - Step by Step
The pipeline script is available under `dags/` folder as `main_dag.py`
1. **retrieve_raw_json** - The pipeline starts accessing Breweries API retrieving the data that we want to consume and stores it on `data_lake/bronze/raw`
2. **wait_file** - This step will wait the file to be available on the folder, the pipeline will continue only when the file ready to be read
3. **check_json_quality** - This branch operator will check if the file available on raw folder is right formatted as JSON and can be consumed by the following steps
4. **Branching** - If `check_json_file` make sure that the file can be consumed as JSON, it will execute `move_json_file` and skip `process_json`, if not the it will execute `process_json` and skip `move_json_file`
    * **move_json_file** - As the file is ready to be read by spark, we only need to move the file to the folder `bronze/fixed` where it will be read by spark task
    * **process_json** - When the process file is not ready to be read, this task will process the file to make necessary changes and format the file to be on JSON format and can be read by spark and finally save the fixed file under `bronze/fixed` folder 
5. **gen_parquet_partition_by_location** - This task triggers the container `silver-processing` which executes a python function that consumes the file available on `bronze/fixed` as JSON and save as parquet format partitioned by location (the location chosen was `state`) under the path `silver/`
6. **gen_view_by_brewery_and_location** - This task triggers the container `golder-processing` which executes the python function that consumes the file parquet file available on `silver/`, aggregate it by brewery type and location counting each one and then saving this file under `gold/` folder as parquet


### Monitoring and Alerting
- The function `notificator` inside `main_dag.py` is set to be triggered whenever a problem occurs on the pipeline, in a real life case this function could be set to send email's, slack messages or some other type of alerting that makes sense to the project
  - This can be tested by excluding the value of the `API_URL` variable inside `.env` file
- The whole process is set to retry 3 times whenever a problem appears and each retry will be delayed in a exponential way, starting with 5 seconds 
