# Brewery Data Pipeline
This pipeline consumes data from Brewery API and loads this data in a data lake following the medallion architecture.
It was developed following "BEES Data Engineering – Breweries Case" challenge
- **Bronze Layer**: stores and persists raw data
- **Silver Layer**: transform data of bronze layer into a parquet format partitioned by location
- **Gold Layer**: Aggregate the parquet files available on silver layer and aggregate by brewery type and location
All data transformations are developed with PySpark and all its distributed processing features, the pipeline is orchestrated by Apache Airflow.

## Instalation and Execution
>[!NOTE]
>This pipeline was developed in Linux so it will be better executed on UNIX based OS's, such as macOS and Linux Distributions. On Windows it is recommended to use WSL with Ubuntu.

>[!TIP]
>Searching for Ubuntu on Microsoft Store you can download and access all Ubuntu feature inside windows, for example run a Ubuntu CLI, which will be the only tool we will need to proceed with instalation.

> [!IMPORTANT]
> Before proceed, make sure that you have the following softwares installed on your system
> * Docker
> * Python 3

To configure this pipeline, follow these steps
1. Clone this repository to your machine
2. Change the permissions of the file `setup_brewery_pipeline.sh` to make it executable
   * Inside the folder where the repository was cloned run `chmod +x setup_brewery_pipeline.sh` 
3. Run the script with `./setup_brewery_pipeline.sh`
   - This will create all necessary folder, install depencies needed and execute Airflow 
   - By default Airflow will run a web service on `localhost:8080` once you access it in your browser a user and password will be request
     * User is `admin` by default
     * Password you can get it on the CLI Lines on the terminal that the setup script was executed or on the file `standalone_admin_password.txt` that is automatically generate on the folder where you cloned this repository
4. Inside Airflow UI you can search for DAG `Brewery_Pipeline` and execute it. This will trigger the process to download and transform brewery data, this can also be triggered via terminal running `airflow dags trigger Brewery_Pipeline`
   
This process will create the folder `data_lake` inside the directory you cloned this repository
- `data_lake`
  - `/bronze`
    - `/raw` persists raw data consumed from API
    - `/fixed` store JSON fixed to be successfully consumed by PySpark
    - `/processed` after fixed files are processed it persists this files
  - `/silver` contains parquet file partitioned by location 
  - `/gold` contains parquet file aggregated by brewery_type and location
  
### Monitoring and Alerting
- The function `notificator` inside `main_dag.py` is set to be triggered whenever a problem occurs on the pipeline, in a real life case this function could be set to send email's, slack messages or some other type of alerting that makes sense to the project
  - This can be tested by excluding the value of the `API_URL` variable inside `.env` file
- The whole process is set to retry 3 times whenever a problem appears and each retry will be delayed in a exponential way, starting with 5 seconds 
