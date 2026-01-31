# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Working with dates
# MAGIC 1. [Date functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#date-and-timestamp-functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Requirement
# MAGIC You are given a dataframe as below

# COMMAND ----------

from pyspark.sql.functions import concat_ws

data_list = [(2022, 5, 18) , (9999, 12, 31), (-9999, 1, 1), (10000, 1, 1), 
             (-10000, 1, 1), (193, 5, 25),   (99, 5, 25),   (1000, 2, 29)]

df = (spark.createDataFrame(data_list).toDF("Y", "M", "D")
     .withColumn("date_str", concat_ws("-", "Y", "M", "D")))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Convert strings to date
# MAGIC
# MAGIC 1. Spark validates the date against the Proleptic Gregorian calendar.
# MAGIC 2. The negative years are BC, and the positive values are AD in Gregorian calander.
# MAGIC 3. Valid dates are taken, and invalid dates throw an exception or taken as null

# COMMAND ----------

from pyspark.sql.functions import expr
df = (
    df.withColumn("valid_date", expr("try_to_date(date_str, 'y-M-d')"))
        .drop("Y", "M", "D")
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Add, Subtract days and months to date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import date_add, date_sub, add_months, date_diff

df = (
    df.withColumns({
        "add_5_days": date_add("valid_date", 5),
        "sub_5_days": date_sub("valid_date", 5),
        "add_5_months": add_months("valid_date", 5),
        "sub_5_months": add_months("valid_date", -5)
    })
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Current date, date difference, and interval

# COMMAND ----------

from pyspark.sql.functions import current_date, date_diff, col
df = (
    df.withColumns({
        "current_date": current_date(),
        "delta_date_days": date_diff("add_5_months", "valid_date"),
        "delta_date_interval": col("current_date") - col("valid_date")
    })
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Format date

# COMMAND ----------

from pyspark.sql.functions import date_format

df = (
    df.withColumn("fmt_date", date_format("valid_date", "dd MMM yyyy"))
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>