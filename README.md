# COVID-19-Data-Analytics-Data-Pipeline

This project is aimed at building a data pipeline for Covid data analytics using various technologies such as Apache Airflow, HDFS, PySpark, Hive, Power BI, and Docker. The pipeline fetches Covid data from GitHub and stages it in HDFS Data Warehouse, which is then transformed using PySpark. The transformed data is loaded into Hive for analytics, and the results are visualized using Power BI. The entire process is executed in Docker containers, ensuring consistency and reliability in the pipeline.The pipeline sends email notifications for success or failure and also posts messages to a Slack channel.

## Project Overview

The objective of this project is to design and build a data pipeline that fetches COVID-19 data from public repositories on GitHub, stages the data in an HDFS data warehouse, transforms the data using PySpark, loads the transformed data into Hive, and visualizes the data using Power BI. The pipeline also sends email notifications for success or failure and posts messages to a Slack channel.

The project involves the following key steps:

Fetch COVID-19 data from GitHub: The project retrieves COVID-19 data from public repositories on GitHub. The data is fetched using APIs and stored in a staging area in HDFS.

Transform the data using PySpark: The data is processed and transformed using PySpark to ensure that it is ready for analysis. PySpark enables fast and efficient processing of the data, which is important given the size of the datasets involved.

Load the transformed data into Hive: The transformed data is loaded into Hive, a data warehouse system for Hadoop. Hive provides a SQL-like interface to query the data stored in HDFS.

Visualize the data using Power BI: The transformed data is visualized using Power BI, a powerful data visualization tool that enables the project to create interactive dashboards and reports.

Monitor and optimize the data pipeline: The data pipeline is monitored and optimized using Apache Airflow, which enables the project to track the progress of the pipeline and identify bottlenecks. The pipeline also sends email notifications for success or failure and posts messages to a Slack channel.

Deploy the project on Docker: The entire project is deployed on Docker to provide a scalable and portable solution.

## Technologies Used
The project uses the following technologies:

Apache Airflow: Used as an orchestration tool for building the data pipeline and for email notifications.
PySpark: Used for data processing and transformation.
HDFS: Used as a data warehouse for storing the staged data.
Hive: Used as a SQL-like interface for querying the transformed data.
Power BI: Used for data visualization and analytics.
Slack: Used for team communication and for posting pipeline messages.
Docker: Used for deploying the entire project.

## Project Benefits
The project provides the following benefits:

Enables the project to quickly and efficiently process large volumes of COVID-19 data.
Provides a SQL-like interface for querying the data.
Enables the project to create interactive dashboards and reports for data visualization and analytics.
Provides a scalable and reliable data pipeline that can be monitored and optimized for performance.
Sends email notifications for pipeline success or failure to relevant team members.
Posts pipeline messages to a Slack channel for team communication and monitoring.
Deploys the entire project on Docker for scalability and portability.

## Conclusion

The COVID-19 Data Analytics Data Pipeline project is an essential tool for processing and analyzing COVID-19 data. The project provides a scalable, reliable, and efficient data pipeline that enables fast and accurate data processing, analytics, and visualization. Additionally, the project sends email notifications for success or failure and posts messages to a Slack channel for easy team communication and monitoring. The project is deployed on Docker to provide scalability and portability.
