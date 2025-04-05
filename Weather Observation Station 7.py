# Databricks notebook source
# MAGIC %md
# MAGIC Query the list of CITY names ending with vowels (a, e, i, o, u) from STATION. Your result cannot contain duplicates.
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
    (1, "New York", "NY", 40.7128, -74.0060),
    (2, "Los Angeles", "CA", 34.0522, -118.2437),
    (3, "Chicago", "IL", 41.8781, -87.6298),
    (4, "Houston", "TX", 29.7604, -95.3698),
    (5, "Phoenix", "AZ", 33.4484, -112.0740),
    (6, "San Antonio", "TX", 29.4241, -98.4936),
    (7, "San Diego", "CA", 32.7157, -117.1611),
    (8, "Dallas", "TX", 32.7767, -96.7970),
    (9, "San Jose", "CA", 37.3382, -121.8863),
    (10, "Austin", "TX", 30.2672, -97.7431),
    (11, "Toronto", "ON", 43.65107, -79.347015),
    (12, "London", "ENG", 51.5074, -0.1278),
    (13, "Vancouver", "BC", 49.2827, -123.1207),
    (14, "england", "ENG", 51.5074, -0.1278),
    (15, "Vancouver", "BC", 49.2827, -123.1207)
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
# MAGIC WHERE LOWER(LEFT(City,1)) in ('a','e','i','o','u')

# COMMAND ----------

from pyspark.sql.functions import col, substring, lower

result = df.withColumn("first_letter",lower(substring(df.City,1,1)))
result.where("first_letter in ('a','e','i','o','u')").distinct().select('City').show()