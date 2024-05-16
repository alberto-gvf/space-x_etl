# space-x_etl

## Table of Contents
1. [Description](#Descripition)
1. [Setup](#local-setup)
2. [How to launch the project](#how-to-launch-the-project)


# Description

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

# Setup
- Install python 3.9.1
- Install docker
- Create virtualenv
- Install requirements of the project in requirements.txt file


# How to launch the project

- run docker-compose up
- airflow: localhost:8080
Airflow credentials:
user: admin
password:y you'll find it in the file airflow/standalone_admin_password.txt once airflow is up.
- Jupyter notebook: localhost:80
