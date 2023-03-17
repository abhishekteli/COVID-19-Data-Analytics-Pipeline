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

df_raw_population = spark.read.csv("hdfs://namenode:9000/Covid_Data/population_by_age.tsv", sep=r'\t', header=True)
df_dim_country = spark.read.csv("hdfs://namenode:9000/Covid_Data/country_lookup.csv", sep=r',', header=True)

df_raw_population = df_raw_population.withColumn('age_group', regexp_replace(split(df_raw_population \
                    ['indic_de,geo\\time'], ',')[0], 'PC_', '')).withColumn('country_code', \
                    split(df_raw_population['indic_de,geo\\time'], ',')[1])

df_raw_population = df_raw_population.select(col("country_code").alias("country_code"),
                                             col("age_group").alias("age_group"),
                                             col("2019 ").alias("percentage_2019"))

df_raw_population.createOrReplaceTempView("raw_population")

df_raw_population_pivot = spark.sql("SELECT country_code, age_group, cast(regexp_replace(percentage_2019, '[a-z]', '')  \
        AS decimal(4,2)) AS percentage_2019 FROM raw_population WHERE length(country_code) = 2") \
        .groupBy("country_code").pivot("age_group").sum("percentage_2019").orderBy("country_code")

df_raw_population_pivot.createOrReplaceTempView("raw_population_pivot")


df_dim_country.createOrReplaceTempView("dim_country")

df_processed_population = spark.sql("""SELECT c.country,
       c.country_code_2_digit,
       c.country_code_3_digit,
       c.population,
       p.Y0_14  AS age_group_0_14,
       p.Y15_24 AS age_group_15_24,
       p.Y25_49 AS age_group_25_49,
       p.Y50_64 AS age_group_50_64, 
       p.Y65_79 AS age_group_65_79,
       p.Y80_MAX AS age_group_80_max
  FROM raw_population_pivot p
  JOIN dim_country c ON p.country_code = country_code_2_digit
 ORDER BY country""")

df_processed_population.write.mode("append").insertInto("population")