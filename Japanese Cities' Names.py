# Databricks notebook source
# MAGIC %md
# MAGIC Query the names of all the Japanese cities in the CITY table. The COUNTRYCODE for Japan is JPN.
# MAGIC The CITY table is described as follows:
# MAGIC
# MAGIC ![Output](https://s3.amazonaws.com/hr-challenge-images/8137/1449729804-f21d187d0f-CITY.jpg)

# COMMAND ----------

from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("Japanese Cities Query") \
    .getOrCreate()

# Sample data including Japanese cities
city_data = [
    (1, "Tokyo", "JPN", "Tokyo-to", 13350000),
    (2, "Osaka", "JPN", "Osaka", 2691000),
    (3, "Kyoto", "JPN", "Kyoto", 1475000),
    (4, "Nagoya", "JPN", "Aichi", 2296000),
    (5, "New York", "USA", "New York", 8008278),
    (6, "London", "GBR", "England", 8788000)
]

columns = ["ID", "NAME", "COUNTRYCODE", "DISTRICT", "POPULATION"]

# Create DataFrame
city_df = spark.createDataFrame(city_data, columns)

# Register as a temporary table
city_df.createOrReplaceTempView("CITY")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Name
# MAGIC FROM CITY
# MAGIC WHERE COUNTRYCODE = "JPN"

# COMMAND ----------

city_df.filter("COUNTRYCODE == 'JPN'").select("Name").show()