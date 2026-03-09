# Data Engineering Zoomcamp 2026  
## Homework 6 – Batch Processing with Apache Spark

This repository contains the solution for **Homework 6 – Batch Processing** from the **Data Engineering Zoomcamp 2026** by DataTalksClub.

The project demonstrates how large-scale taxi trip datasets can be processed using **Apache Spark** in a batch processing pipeline. The workflow includes dataset ingestion, transformation, partitioning, analytical queries, and monitoring using the Spark UI.

---

# Project Overview

The goal of this homework is to build a **Spark-based batch processing pipeline** to analyze **NYC Yellow Taxi trip data for November 2025**.

The pipeline processes trip records, performs transformations using the Spark DataFrame API, stores the output as **partitioned Parquet files**, and answers analytical questions about trip durations, record counts, and pickup locations.

---

# Architecture

```text
NYC Taxi Dataset (Nov 2025)
            │
            ▼
     Data Ingestion
     (Spark Read)
            │
            ▼
   Data Transformation
   (PySpark DataFrame)
            │
            ▼
     Repartitioning
     (4 partitions)
            │
            ▼
     Parquet Storage
            │
            ▼
    Analytical Queries
            │
            ▼
       Spark UI
   (Job Monitoring)

Technologies Used
Technology	Purpose
Apache Spark	Distributed batch data processing
PySpark	Python API for Spark
Python	Data processing scripts
Parquet	Columnar storage format
Spark UI	Monitoring and debugging jobs
NYC Taxi Dataset	Real-world data source
Dataset

Dataset used in this project:

NYC Yellow Taxi Trip Records – November 2025

Official Source:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The dataset contains information such as:

Pickup and dropoff timestamps

Trip distance

Payment type

Pickup and dropoff locations

Fare and tip amounts

Spark Processing Pipeline

The batch pipeline follows the steps below:

1. Initialize Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC Taxi Batch Processing") \
    .getOrCreate()
2. Load Taxi Dataset
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

3. Repartition Data

The dataset is repartitioned to improve parallel processing.
df.repartition(4).write.parquet("yellow/2025/11/")
4. Analytical Queries

Example: Count trips for November 15, 2025
from pyspark.sql.functions import to_date

df.filter(to_date("tpep_pickup_datetime") == "2025-11-15").count()

5. Trip Duration Analysis
from pyspark.sql.functions import col

df = df.withColumn(
    "trip_hours",
    (col("tpep_dropoff_datetime").cast("long") -
     col("tpep_pickup_datetime").cast("long")) / 3600
)

df.agg({"trip_hours": "max"}).show()

Homework Answers
Question	Answer
Spark Version	4.1.1
Parquet Output Size	75MB
Trips on Nov 15 2025	162,604
Longest Trip Duration	90.6 hours
Spark UI Port	4040
Least Frequent Pickup Zone	Governor's Island/Ellis Island/Liberty Island
Spark UI

Spark provides a web interface to monitor running jobs.
http://localhost:4040
The UI allows users to inspect:

Job execution

DAG visualization

Task distribution

Memory usage

Execution stages

Repository Structure
de-zoomcamp-hw6
│
├── data
│   └── yellow_tripdata_2025-11.parquet
│
├── scripts
│   └── spark_batch_processing.py
│
├── notebooks
│   └── homework_analysis.ipynb
│
└── README.md

Key Learning Outcomes

This homework demonstrates:

Batch processing using Apache Spark

Working with large-scale datasets

Spark DataFrame transformations

Partitioning strategies for distributed computation

Columnar storage with Parquet

Monitoring jobs using Spark UI

Course

Data Engineering Zoomcamp
https://github.com/DataTalksClub/data-engineering-zoomcamp

Author

Rini Mondal

Data Engineering Zoomcamp Participant
Machine Learning Zoomcamp Graduate