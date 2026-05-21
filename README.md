# Big Data Processing with Apache Spark on Kubernetes

This project implements and benchmarks multiple big data processing queries using **Apache Spark**, **PySpark**, **Spark SQL**, **RDDs**, **DataFrames**, and **Kubernetes-based distributed execution**.

The goal of the project is to process large-scale public datasets, compare different Spark APIs, evaluate performance under different cluster configurations, and analyze how Spark chooses physical execution plans and join strategies.

This repository is structured as a practical big data engineering project and demonstrates hands-on experience with distributed data processing, Spark optimization, Kubernetes execution, and performance analysis.

## Project Overview

The project focuses on distributed data processing using Apache Spark in a Kubernetes environment.

It includes multiple analytical queries implemented with different Spark APIs, including DataFrame API, SQL API, UDFs, and RDDs. The implementations are compared in terms of execution time, optimization behavior, and suitability for different types of workloads.

The project demonstrates:

- Distributed data processing with Apache Spark
- PySpark DataFrame, SQL, UDF, and RDD APIs
- Spark execution on Kubernetes
- HDFS-based input handling
- Performance benchmarking with different executor, core, and memory configurations
- Catalyst optimizer analysis using `explain`
- Join strategy experimentation with Spark hints
- Broadcast joins, sort-merge joins, shuffled hash joins, nested loop joins, and Cartesian products
- Debugging Spark jobs through Kubernetes driver pod logs

## Datasets

The project uses public datasets related to Los Angeles crime incidents, police stations, census population data, and income statistics.

The datasets are expected to be available through HDFS paths configured in `src/common.py`.

### Crime Data

The main crime dataset contains reported crime incidents in Los Angeles.  
It includes fields such as:

- Crime identifier
- Date and time information
- Crime location
- Latitude and longitude
- Area / division information

This dataset is used mainly in Query 1, Query 2, and Query 4.

### Police Stations Data

The police stations dataset contains information about Los Angeles police divisions and their geographic coordinates.

It includes fields such as:

- Police division name
- Station longitude
- Station latitude

This dataset is used in Query 4 to calculate the nearest police station for each crime incident.

### Census Blocks Data

The census blocks dataset contains population information by geographic area.

It includes fields such as:

- ZIP Code / ZCTA
- Population

This dataset is used in Query 3 to calculate the total population per ZIP code.

### Income Data

The income dataset contains estimated median household income by ZIP code.

It includes fields such as:

- ZIP Code
- Estimated median household income

This dataset is used in Query 3 together with census population data.

## Technologies Used

- Python
- PySpark
- Apache Spark
- Spark SQL
- RDD API
- DataFrame API
- Kubernetes
- HDFS
- Bash scripting
- Git / GitHub

## Repository Structure

```text
.
├── README.md
├── LLM_USAGE.md
├── scripts/
│   ├── submit_k8s.sh
│   └── submit_local.sh
└── src/
    ├── common.py
    ├── convert_crime_csv_to_parquet.py
    ├── query1_df.py
    ├── query1_df_parquet.py
    ├── query1_df_udf.py
    ├── query1_rdd.py
    ├── query2_df.py
    ├── query2_sql.py
    ├── query3_df.py
    ├── query3_df_hints.py
    ├── query3_rdd.py
    ├── query4_df.py
    └── query4_df_hints.py
