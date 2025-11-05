## Overview
This project demonstrates a simple yet powerful data pipeline using Apache Airflow. The pipeline reads a CSV file from Google Cloud Storage (GCS), loads it into a BigQuery table, performs data cleaning and transformation, joins tables, and finally exports the resulting dimension table back to GCS as a CSV file.
## Technologies Used
- Apache Airflow
- Google Cloud Storage
- Google BigQuery
- Python
- BigQuery Operators from Airflow

## Pipeline Workflow
1. Read CSV from GCS
The pipeline starts by reading a CSV file stored in a GCS bucket.

2. Load into BigQuery
The file is loaded into a staging table in BigQuery using BigQueryInsertJobOperator.

3. Check Table Creation
A check is performed using BigQueryTableCheckOperator to ensure the table was created successfully.

4. Clean the Table
Data cleaning operations are applied—such as removing nulls, filtering rows, or standardizing formats.

5. Join Tables
The cleaned table is joined with other reference tables to create a meaningful dimension table.

6. Export to CSV
The final dimension table is exported back to GCS in CSV format using a BigQuery export job.
