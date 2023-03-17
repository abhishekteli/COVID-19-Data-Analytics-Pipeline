from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime, timedelta
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("Covid_Data_Analytics", start_date = datetime(2022, 1, 1), 
    schedule_interval = '@daily', default_args = default_args, catchup = False) as dag:

    check_cases_death_file_exists = HttpSensor(
    task_id='check_cases_death_file_exists',
    http_conn_id='github',
    endpoint='cloudboxacademy/covid19/main/ecdc_data/cases_deaths.csv',
    method='HEAD',
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag
    )

    check_hospital_admissions_file_exists = HttpSensor(
    task_id='check_hospital_admissions_file_exists',
    http_conn_id='github',
    endpoint='cloudboxacademy/covid19/main/ecdc_data/hospital_admissions.csv',
    method='HEAD',
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag
    )

    check_testing_file_exists = HttpSensor(
    task_id='check_testing_file_exists',
    http_conn_id='github',
    endpoint='cloudboxacademy/covid19/main/ecdc_data/testing.csv',
    method='HEAD',
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag
    )

    check_country_response_file_exists = HttpSensor(
    task_id='check_country_response_file_exists',
    http_conn_id='github',
    endpoint='cloudboxacademy/covid19/main/ecdc_data/country_response.csv',
    method='HEAD',
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=dag
    )

    def download_file_from_github(url, output_file):
        # Set the headers for the HTTP request
        headers = {'Accept': 'application/vnd.github.v3.raw'}

        # Send the HTTP request to the GitHub API
        response = requests.get(url, headers=headers)

        # Write the file data to a local file
        with open(output_file, 'w') as f:
            f.write(response.content.decode('utf-8'))

    def download_files():
    
        files_to_download = [
        {
            'url': 'https://raw.githubusercontent.com/cloudboxacademy/covid19/main/ecdc_data/cases_deaths.csv',
            'output_file': '/opt/airflow/dags/files/cases_deaths.csv'
        },
        {
            'url': 'https://raw.githubusercontent.com/cloudboxacademy/covid19/main/ecdc_data/hospital_admissions.csv',
            'output_file': '/opt/airflow/dags/files/hospital_admissions.csv'
        },
        {
            'url': 'https://raw.githubusercontent.com/cloudboxacademy/covid19/main/ecdc_data/testing.csv',
            'output_file': '/opt/airflow/dags/files/testing.csv'
        },
        {
            'url': 'https://raw.githubusercontent.com/cloudboxacademy/covid19/main/ecdc_data/country_response.csv',
            'output_file': '/opt/airflow/dags/files/country_response.csv'
        }

        ]

        for file in files_to_download:
            download_file_from_github(file['url'], file['output_file'])

    download_csv = PythonOperator(
        task_id='download_csv',
        python_callable=download_files,
        dag=dag
    )

    saving_files = BashOperator(
        task_id = 'saving_files',
        bash_command = """
            hdfs dfs -mkdir -p /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/cases_deaths.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/testing.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/country_response.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/hospital_admissions.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/country_lookup.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/dim_date.csv /Covid_Data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/population_by_age.tsv /Covid_Data
        """
    )

    # house_keeping = BashOperator(
    #     task_id = 'house_keeping',
    #     bash_command = """
    #     hadoop fs -rm -r /testing.csv
    #     hadoop fs -rm -r /hospital_admissions.csv
    #     hadoop fs -rm -r /Covid_Data/ccountry_response.csv
    #     """
    # )

    creating_cases_deaths_table = HiveOperator(
        task_id="creating_cases_deaths_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS cases_and_deaths (
                country string,
                country_code_2_digit string,
                country_code_3_digit string,
                population bigint,
                reported_date timestamp,
                cases_count bigint,
                deaths_count bigint,
                source string
            );
        """
    )

    transfor_load_cases_deaths_data = SparkSubmitOperator(
        task_id = 'transfor_load_cases_deaths_data',
        application="/opt/airflow/dags/scripts/covid_cases_deaths_transformation.py",
        conn_id="spark_conn",
        verbose=False
    )

    creating_weekly_hospital_admissions_table = HiveOperator(
        task_id="creating_weekly_hospital_admissions_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS weekly_hospital_admissions (
            country STRING,
            country_code_2_digit STRING,
            country_code_3_digit STRING,
            population INT,
            reported_year_week STRING,
            reported_week_start_date DATE,
            reported_week_end_date DATE,
            new_hospital_occupancy_count DOUBLE,
            new_ICU_occupancy_count DOUBLE,
            source STRING
            );
        """
    )

    creating_daily_hospital_admissions_table = HiveOperator(
        task_id="creating_daily_hospital_admissions_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS daily_hospital_admissions (
            country STRING,
            country_code_2_digit STRING,
            country_code_3_digit STRING,
            population INT,
            reported_date timestamp,
            hospital_occupancy_count DOUBLE,
            ICU_occupancy_count DOUBLE,
            source STRING
            );
        """
    )

    transform_load_hospital_data = SparkSubmitOperator(
        task_id = 'transform_load_hospital_data',
        application="/opt/airflow/dags/scripts/covid_hospital_admissions_transformation.py",
        conn_id="spark_conn",
        verbose=False
    )

    creating_testing_table = HiveOperator(
        task_id="creating_testing_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS testing (
                country STRING,
                country_code_2_digit STRING,
                country_code_3_digit STRING,
                new_cases INT,
                tests_done INT,
                population INT,
                testing_rate DOUBLE,
                positivity_rate DOUBLE,
                year_week STRING,
                week_start_date TIMESTAMP,
                week_end_date TIMESTAMP,
                testing_data_source STRING
            );

        """
    )

    transform_load_testing_data = SparkSubmitOperator(
        task_id = 'transform_load_testing_data',
        application="/opt/airflow/dags/scripts/covid_testing_transformation.py",
        conn_id="spark_conn",
        verbose=False
    )
    
    creating_population_table = HiveOperator(
        task_id="creating_population_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE TABLE IF NOT EXISTS population (
                country string,
                country_code_2_digit string,
                country_code_3_digit string,
                population string,
                age_group_0_14 decimal(14,2),
                age_group_15_24 decimal(14,2),
                age_group_25_49 decimal(14,2),
                age_group_50_64 decimal(14,2),
                age_group_65_79 decimal(14,2),
                age_group_80_max decimal(14,2)
            );

        """
    )

    transform_load_population_data = SparkSubmitOperator(
        task_id = 'transform_load_population_data',
        application="/opt/airflow/dags/scripts/covid_population_transformation.py",
        conn_id="spark_conn",
        verbose=False
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="youremail@example.com",
        subject="Covid_Data_Analytics",
        html_content="<h3>Covid_Data_Analytics</h3>"
    )

    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message=_get_message(),
        channel="#monitoring"
    )

    check_cases_death_file_exists >> check_hospital_admissions_file_exists >> check_testing_file_exists
    check_testing_file_exists >> check_country_response_file_exists >> download_csv >> saving_files >> creating_cases_deaths_table 
    creating_cases_deaths_table >> transfor_load_cases_deaths_data >> creating_weekly_hospital_admissions_table
    creating_weekly_hospital_admissions_table >> creating_daily_hospital_admissions_table >> transform_load_hospital_data
    transform_load_hospital_data >> creating_testing_table >> transform_load_testing_data
    transform_load_testing_data >> creating_population_table >> transform_load_population_data
    transform_load_population_data >> send_email_notification >> send_slack_notification
