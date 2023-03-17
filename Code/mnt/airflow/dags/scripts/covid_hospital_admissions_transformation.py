from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Covid Hospital Admissions Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

hospital_data = spark.read.csv("hdfs://namenode:9000/Covid_Data/hospital_admissions.csv", header = True, inferSchema = True)
dim_date = spark.read.csv("hdfs://namenode:9000/Covid_Data/dim_date.csv", header = True, inferSchema = True)
country_lookup = spark.read.csv("hdfs://namenode:9000/Covid_Data/country_lookup.csv", header = True, inferSchema = True)

hospital_data = hospital_data.drop('url')
hospital_data = hospital_data.withColumnRenamed('date', 'reported_date') \
    .withColumnRenamed('year_week','reported_year_week')

hospital_data.createOrReplaceTempView('hospital_data')
country_lookup.createOrReplaceTempView('country_lookup')

hospital_data = spark.sql("""

select a.*, b.country_code_2_digit, b.country_code_3_digit, b.population
from hospital_data a inner join country_lookup b
on a.country = b.country

""")

hospital_data.createOrReplaceTempView('hospital_data')

weekly_hospital_data = spark.sql("""

select * from hospital_data where indicator like 'Weekly%'

""")
                                 
daily_hospital_data = spark.sql("""

select * from hospital_data where indicator like 'Daily%'

""")

dim_date.createOrReplaceTempView("dim_date")

dim_date = spark.sql("""

select *, concat(year, '-W' , lpad(week_of_year,2,'0')) as ecdc_year_week from dim_date

""")
                     
dim_date = dim_date.withColumn('date', to_date(dim_date.date))

min_date_df = dim_date.groupBy('ecdc_year_week').agg(min('date').alias('min_date'))
dim_date = dim_date.join(min_date_df, on='ecdc_year_week')
dim_date = dim_date.withColumn('week_start_date', dim_date['min_date'])

min_date_df = dim_date.groupBy('ecdc_year_week').agg(max('date').alias('max_date'))
dim_date = dim_date.join(min_date_df, on='ecdc_year_week')
dim_date = dim_date.withColumn('week_end_date', dim_date['max_date'])

dim_date = dim_date.drop('min_date','max_date')

dim_date.createOrReplaceTempView("dim_date")
weekly_hospital_data.createOrReplaceTempView('weekly')

weekly_hospital_data = spark.sql("""

select a.*, b.week_start_date, b.week_end_date
from weekly a inner join dim_date b
on a.reported_year_week = b.ecdc_year_week

""")
                                 
weekly_hospital_data = weekly_hospital_data.groupBy("country","reported_year_week","source" \
                        ,"country_code_2_digit", "country_code_3_digit", "population" \
                        ,"week_start_date", "week_end_date").pivot("indicator").sum("value")

daily_hospital_data = daily_hospital_data.groupBy("country","reported_date","source" \
                      ,"country_code_2_digit","country_code_3_digit" \
                      ,"population").pivot("indicator").sum("value")

weekly_hospital_data = weekly_hospital_data.orderBy(desc('reported_year_week')) \
                        .orderBy(asc('country'))

daily_hospital_data = daily_hospital_data.orderBy(desc('reported_date')).orderBy(asc('country'))

daily_hospital_data = daily_hospital_data.withColumnRenamed('Daily hospital occupancy', 
                                                            'hospital_occupancy_count')

daily_hospital_data = daily_hospital_data.withColumnRenamed('Daily ICU occupancy',
                                                            'ICU_occupancy_count')

daily_hospital_data = daily_hospital_data.select('country', 'country_code_2_digit', 'country_code_3_digit' 
                                                 ,'population', 'reported_date', 'hospital_occupancy_count'
                                                 , 'ICU_occupancy_count', 'source')


weekly_hospital_data = weekly_hospital_data.withColumnRenamed('Weekly new hospital admissions per 100k', 
                                                            'new_hospital_occupancy_count')

weekly_hospital_data = weekly_hospital_data.withColumnRenamed('Weekly new ICU admissions per 100k', 
                                                            'new_ICU_occupancy_count')

weekly_hospital_data = weekly_hospital_data.withColumnRenamed('week_start_date', 
                                                            'reported_week_start_date')

weekly_hospital_data = weekly_hospital_data.withColumnRenamed('week_end_date', 
                                                            'reported_week_end_date')

weekly_hospital_data = weekly_hospital_data.select('country', 'country_code_2_digit', 'country_code_3_digit',
                                                   'population', 'reported_year_week','reported_week_start_date', 
                                                   'reported_week_end_date','new_hospital_occupancy_count',
                                                   'new_ICU_occupancy_count', 'source')

weekly_hospital_data.write.mode("append").insertInto("weekly_hospital_admissions")
daily_hospital_data.write.mode("append").insertInto("daily_hospital_admissions")




