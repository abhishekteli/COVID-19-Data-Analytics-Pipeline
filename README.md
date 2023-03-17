# COVID-19-Data-Analytics-Data-Pipeline

This project is aimed at building a data pipeline for Covid data analytics using various technologies such as Apache Airflow, HDFS, PySpark, Hive, Power BI, and Docker. The pipeline fetches Covid data from GitHub and stages it in HDFS Data Warehouse, which is then transformed using PySpark. The transformed data is loaded into Hive for analytics, and the results are visualized using Power BI. The entire process is executed in Docker containers, ensuring consistency and reliability in the pipeline.

## Project Overview

The objective of this project is to design and build a data pipeline that fetches COVID-19 data from public repositories on GitHub, stages the data in an HDFS data warehouse, transforms the data using PySpark, loads the transformed data into Hive, and visualizes the data using Power BI.

The project involves the following key steps:

Fetch COVID-19 data from GitHub: The project will retrieve COVID-19 data from public repositories on GitHub. The data will be fetched using APIs and stored in a staging area in HDFS.

Transform the data using PySpark: The data will be processed and transformed using PySpark to ensure that it is ready for analysis. PySpark will enable fast and efficient processing of the data, which is important given the size of the datasets involved.

Load the transformed data into Hive: The transformed data will be loaded into Hive, a data warehouse system for Hadoop. Hive will provide a SQL-like interface to query the data stored in HDFS.

Visualize the data using Power BI: The transformed data will be visualized using Power BI, a powerful data visualization tool that will enable the project to create interactive dashboards and reports.

Monitor and optimize the data pipeline: The data pipeline will be monitored and optimized using Apache Airflow, which will enable the project to track the progress of the pipeline and identify bottlenecks.

The entire process is executed in Docker containers, which provide an isolated environment for running the pipeline. Docker ensures that the pipeline is consistent and reliable, regardless of the environment it is run in.

## Technologies Used
The project uses the following technologies:

Apache Airflow: Used as an orchestration tool for building the data pipeline.
PySpark: Used for data processing and transformation.
HDFS: Used as a data warehouse for storing the staged data.
Hive: Used as a SQL-like interface for querying the transformed data.
Power BI: Used for data visualization and analytics.

## Project Benefits
The project provides the following benefits:

Enables the project to quickly and efficiently process large volumes of COVID-19 data.
Provides a SQL-like interface for querying the data.
Enables the project to create interactive dashboards and reports for data visualization and analytics.
Provides a scalable and reliable data pipeline that can be monitored and optimized for performance.

## Conclusion
In conclusion, this project showcases a complete data pipeline for Covid data analytics using various technologies such as Apache Airflow, HDFS, PySpark, Hive, Power BI, and Docker. The pipeline is designed to automate the entire process, from data ingestion to visualization, and can be easily extended to support other data sources and use cases.
