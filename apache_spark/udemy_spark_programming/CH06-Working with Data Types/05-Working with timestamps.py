# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Working with timestamp
# MAGIC 1. [Timestamp functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#date-and-timestamp-functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How Spark stores the timestamp
# MAGIC
# MAGIC 1. Timestamp is internally stored as 12 byte integer known as INT96
# MAGIC 2. Timestamp is made up of 7 fields
# MAGIC     1. Year
# MAGIC     2. Month
# MAGIC     3. Day
# MAGIC     4. Hour
# MAGIC     5. Minute
# MAGIC     6. Second
# MAGIC         * Up to 6 decimal places
# MAGIC         * Microsecond precision
# MAGIC     7. Timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Requirement
# MAGIC You are given the below dataframes

# COMMAND ----------

data_list_1 = [(1, "2022-05-18T10:30:30.0000"), (2, "2022-05-19T11:30:10.0000")]
data_list_2 = [(1, "18-05-2022 10:30:30.0000"), (2, "19-05-2022 10:30:10.0000")]
data_list_3 = [(1, "2022-05-18 10:30:30.0000"), (2, "19-05-2022 10:30:10.0000")]

df_1 = (spark.createDataFrame(data_list_1).toDF("id", "string_time"))
df_2 = (spark.createDataFrame(data_list_2).toDF("id", "string_time"))
df_3 = (spark.createDataFrame(data_list_3).toDF("id", "string_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 covert the df_1 to timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df_1.select("string_time",
             to_timestamp("string_time", "yyyy-MM-dd'T'HH:mm:ss.SSSS").alias("valid_time")
             ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Convert the df_2 to time

# COMMAND ----------

df_2.selectExpr(
    "string_time",
    "to_timestamp(string_time, 'dd-MM-yyyy HH:mm:ss.SSSS') as valid_time"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Convert the df_3 to time
# MAGIC

# COMMAND ----------

df_3.selectExpr(
    "string_time",
    "try_to_timestamp(string_time, 'yyyy-MM-dd HH:mm:ss.SSSS') as valid_time"
).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Timezone information
# MAGIC
# MAGIC 1. A timestamp without timezone information is incomplete.
# MAGIC 2. Spark offers two data types for timestamp
# MAGIC     1. TIMESTAMP
# MAGIC     2. TIMESTAMP_NTZ
# MAGIC 3. For TIMESTAMP, Spark assumes session timezone as the default when timezone is not specified
# MAGIC 4. Session timezone is specified as spark.sql.session.timeZone

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1 What is your default session timezone?
# MAGIC

# COMMAND ----------

spark.conf.get("spark.sql.session.timeZone")

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 Change your session timezone to IST

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", 'Etc/UTC')

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Working with NTZ data

# COMMAND ----------

# MAGIC %md
# MAGIC 4.1 Load machine-events-no-tz.csv file and show the data

# COMMAND ----------

event_ntz_schema = "component string, event_time string, reading string"

event_ntz_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(event_ntz_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/machine-events-no-tz.csv")
)

event_ntz_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.2 Parse te event_time to a TIMESTAMP_NTZ value

# COMMAND ----------

from pyspark.sql.functions import to_timestamp_ntz, lit

event_valid_ntz_df = (
    event_ntz_df.withColumn("event_time_valid_ntz", to_timestamp_ntz("event_time", lit("dd-MM-yyyy HH:mm:ss.SSS")))
)

event_valid_ntz_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.3 event_time_ntz field to a valid timestamp value\
# MAGIC Assume the event_time_ntz is IST time

# COMMAND ----------

from pyspark.sql.functions import convert_timezone, lit

stz = spark.conf.get("spark.sql.session.timeZone")

events_df = (
    event_valid_ntz_df.withColumn("event_time_tz", to_timestamp(convert_timezone(lit("IST"), lit(stz), "event_time_valid_ntz")))
)

events_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Working with TZ data

# COMMAND ----------

# MAGIC %md
# MAGIC 5.1 Load machine-events-with-tz.csv file and show the data

# COMMAND ----------

event_tz_schema = "component string, event_time_tz_str string, reading string"

events_tz_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(event_tz_schema)
    .load("/Volumes/dev/spark_db/datasets/spark_programming/data/machine-events-with-tz.csv")
)

display(events_tz_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 5.2 Parse the event_time field to a valid timestamp value\
# MAGIC Timezone information is provided in the data file

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
event_data_df = (
    events_tz_df.withColumn("event_time_tz", to_timestamp("event_time_tz_str", "dd-MM-yyyy HH:mm:ss.SSSZ"))
)
event_data_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>