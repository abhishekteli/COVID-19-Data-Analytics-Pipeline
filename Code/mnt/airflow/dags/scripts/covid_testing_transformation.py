from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Covid Cases and Death Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

testing_data = spark.read.csv("hdfs://namenode:9000/Covid_Data/testing.csv", header = True, inferSchema = True)
dim_date = spark.read.csv("hdfs://namenode:9000/Covid_Data/dim_date.csv", header = True, inferSchema = True)
country_lookup = spark.read.csv("hdfs://namenode:9000/Covid_Data/country_lookup.csv", header = True, inferSchema = True)

testing_data.createOrReplaceTempView('testing_data')
dim_date.createOrReplaceTempView('dim_date')
country_lookup.createOrReplaceTempView('country_lookup')

testing_data = spark.sql("""

select a.*, b.country_code_2_digit, b.country_code_3_digit
from testing_data a join country_lookup b
on a.country = b.country

""")

testing_data = testing_data.drop('country_code')
dim_date = dim_date.withColumn('date', to_date(dim_date.date))

dim_date = spark.sql("""

select *, concat(year, '-W' , lpad(week_of_year,2,'0')) as ecdc_year_week from dim_date

""")

min_date_df = dim_date.groupBy('ecdc_year_week').agg(min('date').alias('min_date'))
dim_date = dim_date.join(min_date_df, on='ecdc_year_week')
dim_date = dim_date.withColumn('week_start_date', dim_date['min_date'])

min_date_df = dim_date.groupBy('ecdc_year_week').agg(max('date').alias('max_date'))
dim_date = dim_date.join(min_date_df, on='ecdc_year_week')
dim_date = dim_date.withColumn('week_end_date', dim_date['max_date'])

dim_date.createOrReplaceTempView('dim_date')
testing_data.createOrReplaceTempView('testing_data')

testing_data = spark.sql("""

select a.*, b.week_start_date, b.week_end_date
from testing_data a join dim_date b
on a.year_week = b.ecdc_year_week

""")

testing_data = testing_data.select('country','country_code_2_digit','country_code_3_digit','new_cases'
                                   ,'tests_done', 'population', 'testing_rate', 'positivity_rate',
                                  'year_week','week_start_date','week_end_date','testing_data_source')


testing_data.write.mode("append").insertInto("testing")