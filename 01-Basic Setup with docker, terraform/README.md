# PostgreSQL Environment Setup with Docker and Terraform

## Overview

This repository facilitates the setup of a PostgreSQL environment using Docker. The environment is particularly designed for working on Homework 1, and this README provides guidance on building, loading datasets, and adding them to the database.

## Building the PostgreSQL Environment

🚀 To build the PostgreSQL environment, run the following command:

```bash
    docker-compose up -d
```


## Loading Dataset and Adding it to the Database
There are two approaches to load the dataset and add it to the database.

🛠️ There are 2 ways to load the dataset and add it to the database.
## 1. Using Dockerfile
 You can leverage the provided Dockerfile to load the dataset by executing the following steps:
 

 Build the Docker image for HW1 environment
```bash
    docker build -t taxi_ingest_hw1:v001 .
```

 Run the Docker container for HW1:

 ```bash
    gURL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    tURL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    docker run -it \
    --network=pg-network \
    taxi_ingest_hw1:v001 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5432 \
        --db=ny_taxi \
        --gurl=${gURL} \
        --turl=${tURL}

```
Note: The parameters used in the docker run command, such as network, user, password, host, port and db, are based on the configuration specified in the docker-compose.yaml file. Ensure to replace them with accurate values as per your configuration.

## 2. Run the `ingest_HW1.py` directly. 
Alternatively, you can directly run the ingest_HW1.py script without using Dockerfile:

```bash
    gURL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    tURL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    python ingest_HW1.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --gurl=${gURL} \
    --turl=${tURL}

```
Note: The parameters used in the docker run command, such as user, password, host, port and db, are based on the configuration specified in the docker-compose.yaml file. Ensure to replace them with accurate values as per your configuration.

