# Databricks notebook source
# MAGIC %md
# MAGIC Query all columns (attributes) for every row in the CITY table.
# MAGIC
# MAGIC The CITY table is described as follows:
# MAGIC
# MAGIC ![Output](https://s3.amazonaws.com/hr-challenge-images/8137/1449729804-f21d187d0f-CITY.jpg)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("CityQuery").getOrCreate()

# Sample data
data = [
    (1, "New York", "USA", "New York", 8008278),
    (2, "Los Angeles", "USA", "California", 3694820),
    (3, "Chicago", "USA", "Illinois", 2896016),
    (4, "Houston", "USA", "Texas", 1953631),
    (5, "Phoenix", "USA", "Arizona", 1445632)
]

# Define schema
columns = ["ID", "Name", "CountryCode", "District", "Population"]

# Create DataFrame
city_df = spark.createDataFrame(data, columns)

# Show the data
city_df.show()

city_df.createOrReplaceTempView("CITY")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM CITY

# COMMAND ----------

city_df.show()


# COMMAND ----------


city_df.select("ID", "Name", "CountryCode", "District", "Population").show()