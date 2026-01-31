# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement
# MAGIC 1. Load data from flight-time.json into a table
# MAGIC 2. Table structure is given below
# MAGIC
# MAGIC ```
# MAGIC     FL_DATE DATE, 
# MAGIC     OP_CARRIER STRING, 
# MAGIC     OP_CARRIER_FL_NUM STRING, 
# MAGIC     ORIGIN STRING, 
# MAGIC     ORIGIN_CITY_NAME STRING, 
# MAGIC     DEST STRING, 
# MAGIC     DEST_CITY_NAME STRING, 
# MAGIC     CRS_DEP_TIME LONG, 
# MAGIC     DEP_TIME LONG, 
# MAGIC     WHEELS_ON INT, 
# MAGIC     TAXI_IN INT, 
# MAGIC     CRS_ARR_TIME LONG, 
# MAGIC     ARR_TIME LONG, 
# MAGIC     CANCELLED INT, 
# MAGIC     DISTANCE INT
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data from the flight-time.json file

# COMMAND ----------

flight_time_raw_df = (
    spark.read
        .format("json")
        .option("mode", "FAILFAST")
        .option("dateFormat", "M/d/yyyy")
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Investigate the dataframe data and schema for problems

# COMMAND ----------

flight_time_raw_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Define Dataframe schema before reading it

# COMMAND ----------

from pyspark.sql.types import StringType, LongType, IntegerType, DateType, StructType, StructField

flight_schema = StructType([
    StructField("FL_DATE", DateType()),
    StructField("OP_CARRIER", StringType()),
    StructField("OP_CARRIER_FL_NUM", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("ORIGIN_CITY_NAME", StringType()),
    StructField("DEST", StringType()),
    StructField("DEST_CITY_NAME", StringType()),
    StructField("CRS_DEP_TIME", LongType()),
    StructField("DEP_TIME", LongType()),
    StructField("WHEELS_ON", IntegerType()),
    StructField("TAXI_IN", IntegerType()),
    StructField("CRS_ARR_TIME", LongType()),
    StructField("ARR_TIME", LongType()),
    StructField("CANCELLED", IntegerType()),
    StructField("DISTANCE", IntegerType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Read datafile with schema-on-read

# COMMAND ----------

flight_time_raw_with_schema_df = (
    spark.read
        .format("json")
        .option("mode", "FAILFAST")
        .option("dateFormat", "M/d/yyyy")
        .schema(flight_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Investigate the Dataframe data and schema for problems

# COMMAND ----------

flight_time_raw_with_schema_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Save the Dataframe to the table flight_time_raw

# COMMAND ----------

flight_time_raw_with_schema_df.write.mode("overwrite").saveAsTable("dev.spark_db.flight_time_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.spark_db.flight_time_raw limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC