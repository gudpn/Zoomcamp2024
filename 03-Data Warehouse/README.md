# BigQuery Guide for Homework 3 

## Overview
Welcome to the enchanting world of BigQuery! This README aims to provide detailed instructions for establishing your environment using Mage and Google Cloud Platform (GCP) for Homework 3. The exercise is structured into two integral parts:

1. **Pipeline Construction for Data Manipulation**
    - Loading and exporting data into GCP Buckets specifically for the pivotal year 2020.

2. **Leveraging BigQuery Capabilities**
    - a) Establishing an external table utilizing Green Taxi Trip Records Data for the forward-looking year 2022.
    - b) Creating a table in BigQuery with Green Taxi Trip Records for 2022, avoiding partitioning or clustering.

## Setting Up the Mage Environment 🪄

Initiate the integration process by meticulously following the instructions outlined in the [Mage-Zoomcamp Guide](https://github.com/mage-ai/mage-zoomcamp). This guide serves as the authoritative reference for cloning the repository and orchestrating Docker Compose to meticulously set up your professional environment.

## Preparing for the Exercise of Excellence
### Google Cloud Platform (GCP)
For a seamless integration, a GCP environment is imperative, inclusive of a GCP Bucket and a meticulously configured BigQuery setup. Terraform scripts (`main.tf` and `variable.tf`) have been thoughtfully provided to automate this setup process. Attain the necessary credentials by generating a new Cloud key, meticulously endowed with permissions for both reading and writing to BigQuery and the GCP Bucket. Precisely specify the key's location in the `io.config` file within the Mage environment. For detailed implementation, consult the relevant [Week 2](02-Workflow%20Orchestration%20with%20Mage/) documentation.

### Data Transformation and Export: Parquet Files into BigQuery

A pipeline is at your disposal for seamless integration.

1. **parquet_to_gcs**

    - **Data Loader**: Efficiently uses Pandas to read 12 parquet files for the year 2020, carefully defining column types.
    - **Data Transformer**: Introduces a pickup date column, an essential component for data manipulation during the exercise.
    - **Data Exporter**: Employs a data exporter to seamlessly transfer data into Parquet files, securely housed within the GCP Bucket.

# BigQuery Environment
Upon completing the pipeline, the parquet files will store in the GCP Cloud Bucket.

Run the provided script in the designated folder to reveal results. 
