# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Working with String
# MAGIC 1. [String Manipulation Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Requirement
# MAGIC You are given a dataframe below
# MAGIC ```
# MAGIC   df = spark.createDataFrame([("25","07","1982")]).toDF("day", "month", "year")
# MAGIC ```
# MAGIC Create a date from day, month, and year.

# COMMAND ----------

from pyspark.sql.functions import concat_ws, to_date

df = spark.createDataFrame([("25","07","1982")]).toDF("day", "month", "year")
#df.display()

date_expr = to_date(concat_ws("-", "year", "month", "day"))

df.select(date_expr.alias("date")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Requirement
# MAGIC You are given a dataframe below
# MAGIC ```
# MAGIC   df = (spark.createDataFrame([("Sanjay", "Kalra", 25, "July", 1982, 19408.98)])
# MAGIC           .toDF("fist_name", "last_name", "day", "month", "year", "salary"))
# MAGIC ```
# MAGIC Create the following output
# MAGIC ```
# MAGIC   +-----------------------------------------------+----------+
# MAGIC   |fun_text                                       |salary    |
# MAGIC   +-----------------------------------------------+----------+
# MAGIC   |Mr. Sanjay Kalra was born on 25th July of 1982.|$19,408.98|
# MAGIC   +-----------------------------------------------+----------+
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import format_number, format_string, col

df = (spark.createDataFrame([("Sanjay", "Kalra", 25, "July", 1982, 19408.98)])
           .toDF("first_name", "last_name", "day", "month", "year", "salary"))

salary_fmt =  format_number("salary", "$###,###.##").alias("salary")

text_fmt = format_string("Mr. %s %s was born on %dth %s of %d.",
                         "first_name", "last_name", "day", "month", "year").alias("fun_text")

df.select(text_fmt, salary_fmt).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Requirement
# MAGIC You are given a dataframe below
# MAGIC ```
# MAGIC   df = spark.createDataFrame([("Benga`li Market", "110001"),("Adu~godi", "560030")]) \
# MAGIC           .toDF("address", "pin")
# MAGIC ```
# MAGIC Write code to fix the data problem in the address field

# COMMAND ----------

from pyspark.sql.functions import translate

df = spark.createDataFrame([("Benga`li Market", "110001"),("Adu~godi", "560030")]) \
          .toDF("address", "pin")

df.withColumn("address", translate("address", "`~", "")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>