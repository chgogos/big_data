# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement
# MAGIC Read data from flight_time and transform as below
# MAGIC 1. Rename fl_date to dep_date
# MAGIC 2. Compute arr_date
# MAGIC 3. Following fields to represent full timestamp
# MAGIC     1. crs_dep_time
# MAGIC     2. dep_time
# MAGIC     3. crs_arr_time
# MAGIC     4. arr_time
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.spark_db.flight_time order by DISTANCE desc

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 1. Can we do it using select() or selectExpr() transformations?

# COMMAND ----------

flight_time_df = spark.read.table("dev.spark_db.flight_time")

flight_time_1_df = (
    flight_time_df.selectExpr(
        "fl_date as dep_date",
        "to_date(dep_date + dep_time + wheels_on + taxi_in) as arr_date",
        "dep_date + crs_dep_time as crs_dep_time",
        "dep_date + dep_time as dep_time",
        "arr_date + crs_arr_time as crs_arr_time",
        "arr_date + arr_time as arr_time",
        "op_carrier"
    )
)

flight_time_1_df.where("op_carrier_fl_num = 1451 and dep_date = '2000-01-01'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Can we do it using withColumn() or withColumns()?

# COMMAND ----------

from pyspark.sql.functions import expr

flight_time_2_df = (
  flight_time_df.withColumnRenamed("fl_date", "dep_date")
      .withColumn("arr_date", expr("to_date(dep_date + dep_time + wheels_on + taxi_in) as arr_date"))
      .withColumns({
        "crs_dep_time": expr("dep_date + crs_dep_time"),
        "dep_time": expr("dep_date + dep_time"),
        "crs_arr_time": expr("arr_date + crs_arr_time"),
        "arr_time": expr("arr_date + arr_time"),
      })
)

flight_time_2_df.where("op_carrier_fl_num = 1451 and dep_date = '2000-01-01'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Alternative approach to write expressions\
# MAGIC   Why to use it?
# MAGIC     * It gives access to column functions

# COMMAND ----------

from pyspark.sql.functions import to_date, col

flight_time_2_df = (
  flight_time_df.withColumnRenamed("fl_date", "dep_date")
      .withColumn("arr_date", to_date(col("dep_date") + col("dep_time") + col("wheels_on") + col("taxi_in")))
      .withColumns({
        "crs_dep_time": col("dep_date") + col("crs_dep_time"),
        "dep_time": col("dep_date") + col("dep_time"),
        "crs_arr_time": col("arr_date") + col("crs_arr_time"),
        "arr_time": col("arr_date") + col("arr_time"),
      })
)

flight_time_2_df.where((col("op_carrier_fl_num") == 1451) & (col("dep_date") == '2000-01-01')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC
# MAGIC