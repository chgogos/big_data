# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement
# MAGIC 1. Read raw data from flight_time_raw table
# MAGIC 2. Apply transformations to time values as hour to minute interval
# MAGIC
# MAGIC     1. CRS_DEP_TIME
# MAGIC     2. DEP_TIME
# MAGIC     3. WHEELS_ON
# MAGIC     4. CRS_ARR_TIME
# MAGIC     5. ARR_TIME
# MAGIC 3. Apply transformation to TAXI_IN to make it a minute interval

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.spark_db.flight_time_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data to create a dataframe

# COMMAND ----------

flight_time_raw_df = spark.read.table("dev.spark_db.flight_time_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Develop logic to transform CRS_DEP_TIME to an interval

# COMMAND ----------

from pyspark.sql.functions import expr

step_1_df = (
    flight_time_raw_df.withColumns({
    "CRS_DEP_TIME_HH": expr("left(lpad(CRS_DEP_TIME, 4, '0'), 2)"),
    "CRS_DEP_TIME_MM": expr("right(lpad(CRS_DEP_TIME, 4, '0'), 2)"),
    })
)

step_2_df = (
    step_1_df.withColumns({
        "CRS_DEP_TIME_NEW": expr("cast(concat(CRS_DEP_TIME_HH, ':', CRS_DEP_TIME_MM) AS INTERVAL HOUR TO MINUTE)")
    })
)

# COMMAND ----------

step_2_df.limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Develop a reusable function

# COMMAND ----------

 def get_interval(hhmm_value):
     from pyspark.sql.functions import expr

     return expr(f"""
                 cast(concat(left(lpad({hhmm_value}, 4, '0'), 2), ':', 
                             right(lpad({hhmm_value}, 4, '0'), 2)) 
                             AS INTERVAL HOUR TO MINUTE)
                 """)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Apply function to dataframe

# COMMAND ----------

result_df = (
    flight_time_raw_df.withColumns({
        "CRS_DEP_TIME": get_interval("CRS_DEP_TIME"),
        "DEP_TIME": get_interval("DEP_TIME"),
        "WHEELS_ON": get_interval("WHEELS_ON"),
        "CRS_ARR_TIME": get_interval("CRS_ARR_TIME"),
        "ARR_TIME": get_interval("ARR_TIME"),
        "TAXI_IN": expr("cast(TAXI_IN AS INTERVAL MINUTE)")
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Save results to the table 

# COMMAND ----------

result_df.write.mode("overwrite").saveAsTable("dev.spark_db.flight_time")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.spark_db.flight_time

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC