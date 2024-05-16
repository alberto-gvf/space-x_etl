# space-x_etl

## Table of Contents
1. [Description](#Descripition)
2. [Setup](#local-setup)
3. [How to launch the project](#how-to-launch-the-project)
4. [Transition to cloud](#Transition-to-cloud) 


## Description

The current project includes the solution to read data from the space-x API and make it usable to 
answer analytical questions. 

For this, an ETL was implemented. 
- The extraction of the ETL consist on calling the API and dumps the jsons in the folder `resources/raw/`. 
- Transformations consist on processing the raw data extracted on the previous step, transforming it into two different 
datasets (launches and cores). Some basic cleanups are applied, like remove datapoints without ids or dedpulication.
This datasets are then store as parquet tables under resources/stage, partitioned by p_creation_date.
- The loading part of this ETL will load the parquet tables into a postgresDB that can be queried for analysis.

Airflow was chosen as orchestrator.

A notebook is included answering the proposed questions.

Following is the structure of the repo.

```
space-x_etl
├── airflow
│   └── dags
├── notebooks
├── resources
│   ├── raw
│   └── stage
├── src
│   ├── extractor.py
│   ├── transformer.py
│   ├── loader.py
│   └── utils.py
├── tests
├── docker-compose.yml
├── dockerfile
├── init.sql
└── requirements.txt
```

## Setup
- Install python 3.9.1
- Install docker
- Create virtualenv
- Install requirements of the project in requirements.txt file
`pip install -r requirements.txt`


## How to launch the project

- run `docker-compose up`
- Access airflow UI: `localhost:8080`
  - Airflow credentials:
    - user: admin 
    - password: you'll find it in the file `airflow/standalone_admin_password.txt` once airflow is up.
  - Airflow dag has been scheduled to run daily. Catchup option has been deactivated so it won't backfill when it's enabled the first time.
Instead, manual trigger with configuration parameters has been enabled so the tables can be backfilled with one run. 
Trigger the dag with options with the date limits that you want to include:
```json
  { "start_date":"2006-03-01", "end_date":"2024-05-16"}
  ```
- Jupyter notebook: It can be accessed on `localhost:80`. It allows to query the postgres DB where the data has been loaded 
and contains the answers for the analytical questions presented.


### Running scripts manually
Scripts for the ETL can also be run manually. They also need `docker-compose up`, to be able to write to the postgres DB.

There is one change required in the code to run scripts manually.  In the configuration of the db engine.
(file `src/loader.py`, line 14) set `db_host = 'localhost'`

After this, you can run:
```
python src/extractor.py --start_date 2015-01-01 --end_date 2024-06-01 
python src/transformer.py   
python src/loader.py --start_date 2020-01-01 --end_date 2020-06-01   
```

### How to run tests
Tests for the transformer are provided.
```
pytest tests/test_transformer.py
```

## Transition to cloud

Assuming AWS as cloud provider, the presented pipeline could have the following architecture:

#### MWAA for orchestration
Airflow could be provided by MWAA, which is fully managed and easy to operate. 

#### Extraction 
Extraction part of the pipeline could be done by Lambdas. In case of high volumes of data, lambdas can be invoqued in
parallel to scale horizontally, so that each of them queries a sub set of data. This could be a datetime filer, or
based on the pagination of the response. The set of lambdas would write asyncronously the output of the API calls into
S3. This part of the pipeline could be run multiple times a day if needed.

#### Transformation
An EMR cluster would process the data once a day. A spark job running in the cluster reads the json files, processes 
them and stores them as parquet tables in S3.


#### Loading
The presentation layer in AWS would depend on the use cases. A data warehouse approach would imply redshift or snowflake,
Some data quality checks could also be included as part of the spark job for a write-audit-publish approach. Only if DQ
checks pass, data would be published to the presentation layer, whether is the Glue Catalog or any data base.

#### Observability
To add observability to the service, Cloudwatch can be leveraged for monitoring, logging and alerting.
