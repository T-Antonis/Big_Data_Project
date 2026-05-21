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
