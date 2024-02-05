# Workflow Orchestration with Mage 🧙‍♂️

## Overview
This README provides a comprehensive guide for setting up the Mage environment using Docker Compose and GCP (Google Cloud Platform) for Homework 2. The exercise involves creating and managing pipelines to perform ETL (Extract, Transform, Load) operations on green taxi data for the final quarter of 2020.

## Building Mage Environment 🪄

Please follow the instructions in the [Mage-Zoomcamp Guide](https://github.com/mage-ai/mage-zoomcamp) to clone the repository and use Docker Compose for setting up the environment.

## Preparation of the Exercise (GCP)
To facilitate the exercise, a GCP (Google Cloud Platform) environment is required, including a GCP Bucket and BigQuery setup. Terraform scripts (`main.tf` and `variable.tf`) are provided to automate the setup process. Users must generate a new Cloud key with appropriate permissions for reading and writing to BigQuery and the GCP Bucket. The key's location should then be specified in the `io.config` file within the Mage environment.

## Introducing the Scripts 📜

In Mage, three pipelines have been created:

1. **green_taxi_etl_postgresql**

    - **Data Loader**: Utilizes Pandas to read data for the final quarter of 2020 (months 10, 11, 12) and adheres to the datatypes and date parsing methods covered in the course.
    
    - **Data Transformer**: Implements transformations including removing rows with zero passenger count or trip distance, creating a new column for pickup date, renaming columns to Snake Case, and asserting specific conditions.
    
    - **Data Exporter**: Uses a Postgres data exporter (SQL or Python) to write the dataset to a table called "green_taxi" in a schema named "mage" in the PostgreSQL database. The pipeline is scheduled to run daily at 5 AM UTC.

2. **green_taxi_etl_gcs**

    - **Data Loader, Data Transformer and Data Exporter**: Similar to the first pipeline, this one involves a data loader and data transformer. The key distinction lies in the data exporter, which exports data into Parquet files stored in a GCP Bucket. The data is partitioned by the "lpep_pickup_date" field. The README indicates the requirement for Terraform configuration and scheduling the pipeline to run daily at 5 AM UTC.

3. **gcs_to_bigquery**

    - **Data Loader, Data Transformer and Data Exporter**: This pipeline consists of three stages: loading data from a Parquet file, renaming columns from Camel Case to Snake Case (e.g., VendorID to vendor_id), and exporting the data to GCP BigQuery. 
    
    An improvement to be made is loading data from partitioned Parquet files instead of a file containing data from the final quarter of 2020


