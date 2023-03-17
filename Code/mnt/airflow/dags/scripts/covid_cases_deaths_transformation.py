from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Covid Cases and Death Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

cases_deaths = spark.read.csv('hdfs://namenode:9000/Covid_Data/cases_deaths.csv', header = True, inferSchema = True)

country_lookup = spark.read.csv("hdfs://namenode:9000/Covid_Data/country_lookup.csv", header = True, inferSchema = True)

cases_deaths.createOrReplaceTempView('cases_deaths')

Europe_data = spark.sql("select * from cases_deaths where continent = 'Europe' and country is not NULL")

Europe_data = Europe_data.drop('continent').drop('rate_14_day')

Europe_data = Europe_data.withColumnRenamed('date','reported_date')

Europe_data = Europe_data.groupBy('country','country_code','population','reported_date','source'). \
    pivot('indicator').sum('daily_count')

country_lookup.createOrReplaceTempView('country_lookup')

Europe_data.createOrReplaceTempView('Europe_data')

Europe_data = spark.sql("""

select a.*,b.country_code_2_digit
from  Europe_data a inner join country_lookup b
on a.country = b.country

""")

Europe_data = Europe_data.withColumnRenamed('country_code', 'country_code_3_digit')

Europe_data = Europe_data.withColumnRenamed('confirmed cases', 'cases_count')

Europe_data = Europe_data.withColumnRenamed('deaths', 'deaths_count')

Europe_data_selected = Europe_data.select(
  "country",
  "country_code_3_digit",
  "population",
  "reported_date",
  "source",
  "cases_count",
  "deaths_count",
  "country_code_2_digit"
)

Europe_data_selected.write.mode("append").insertInto("cases_and_deaths")


