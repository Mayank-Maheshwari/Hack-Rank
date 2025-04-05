# Databricks notebook source
# MAGIC %md
# MAGIC Query the list of CITY names from STATION which have vowels (i.e., a, e, i, o, and u) as both their first and last characters. Your result cannot contain duplicates.
# MAGIC
# MAGIC Input Format
# MAGIC
# MAGIC The STATION table is described as follows:
# MAGIC
# MAGIC
# MAGIC ![Output](https://s3.amazonaws.com/hr-challenge-images/9336/1449345840-5f0a551030-Station.jpg)
# MAGIC
# MAGIC
# MAGIC where LAT_N is the northern latitude and LONG_W is the western longitude.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder.appName("StationTable").getOrCreate()

# Define the schema for the STATION table
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("LAT_N", FloatType(), True),
    StructField("LONG_W", FloatType(), True)
])

# Create the sample data
data = [
    (1, "Oslo", "OSL001", 59.91, 10.75),
    (2, "Udaipur", "UDA001", 24.58, 73.68),
    (3, "Amsterdam", "AMS001", 52.37, 4.89),
    (4, "Orlando", "ORL001", 28.54, 81.38),
    (5, "Indore", "IND001", 22.72, 75.86),
    (6, "Ahmedabad", "AHM001", 23.03, 72.58),
    (7, "Erie", "ERI001", 42.12, 80.09),
    (8, "Eugene", "EUG001", 44.05, 123.09),
    (9, "Agra", "AGR001", 27.18, 78.01),
    (10, "Albuquerque", "ALB001", 35.08, 106.65),
    (11, "Zurich", "ZUR001", 47.37, 8.55)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Save the DataFrame as a temporary table
df.createOrReplaceTempView("STATION")

# Save as a permanent table (uncomment if needed)
# df.write.mode("overwrite").saveAsTable("STATION")

# Show the DataFrame
df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT CITY
# MAGIC FROM Station
# MAGIC WHERE LOWER(LEFT(City,1)) in ('a','e','i','o','u') and LOWER(RIGHT(City,1)) in ('a','e','i','o','u')

# COMMAND ----------

from pyspark.sql.functions import col, substring, lower

result = df.withColumn("first_letter",lower(substring(df.City,1,1)))
result = result.withColumn("last_letter",lower(substring(df.City,-1,1)))
result = result.where("first_letter in ('a','e','i','o','u')")
result = result.where("last_letter in ('a','e','i','o','u')")

result.distinct().select('City').show()