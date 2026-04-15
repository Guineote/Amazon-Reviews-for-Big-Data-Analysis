# 🚀 Amazon Reviews Big Data & Analytics Pipeline

This project demonstrates a complete Big Data pipeline processing approximately 4 million Amazon reviews locally on Windows. It utilizes Apache Spark (PySpark) for Batch and Micro-Batch processing, PostgreSQL as the curated data warehouse, and Machine Learning (K-Means) for customer profiling.

## 🛠️ Tech Stack
* **Processing:** Apache Spark (PySpark)
* **Storage:** Parquet (Processed Layer), PostgreSQL (Curated Layer)
* **Machine Learning:** PySpark MLlib (Clustering / K-Means)
* **Visualization:** Power BI
* **Environment:** Windows 10/11, Python 3.x, Java 17, Hadoop winutils

## ⚙️ Prerequisites
1. Install **Java JDK 17** and set `JAVA_HOME`.
2. Configure **Hadoop winutils** for Windows and set `HADOOP_HOME`.
3. Install **PostgreSQL** (pgAdmin) and create a database named `amazon_project`.
4. Install Python dependencies: `pip install pyspark psycopg2-binary pandas`.
5. Download the PostgreSQL JDBC driver (`postgresql-42.5.0.jar`).

## 🏃‍♂️ Step-by-Step Execution Instructions

### Step 1: Database Setup
Ensure PostgreSQL is running. Create the target database `amazon_project`. The pipeline will automatically create and overwrite the necessary analytical tables.

### Step 2: Batch Processing (Data Cleaning & Transformation)
This script processes the raw CSV data, performs cleaning (`dropna`), data typing, and saves the highly optimized output as Parquet files.
```bash
python scripts/process.py
```

### Step 3: Analytical Aggregations & Joins
This script reads the optimized Parquet files, performs a Join with a business rule catalog, and calculates aggregated metrics (counts and averages).
```bash
python scripts/analytics.py
```

### Step 4: Streaming Processing Simulation
To simulate a real-time environment, this script runs two threads: one generating micro-batches of CSV files every few seconds, and a PySpark Structured Streaming instance reading and processing them incrementally.
```bash
python scripts/streaming_simulation.py
```

### Step 5: Advanced Analytics (Machine Learning)
This script trains an unsupervised Machine Learning model (K-Means Clustering) to segment customers into 3 profiles based on review length and sentiment. The structured output is then loaded directly into PostgreSQL.
```bash
python scripts/ml_model_and_load.py
```

### Step 6: Dashboarding
1. Open Power BI Desktop.
2. Connect to the local PostgreSQL database (localhost, amazon_project).
3. Load the amazon_analytical_model table.
4. Visualize the clusters and business insights.
