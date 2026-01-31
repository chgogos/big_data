# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data from flight_time table

# COMMAND ----------

flight_time_df = spark.read.table("dev.spark_db.flight_time")
flight_time_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Find the top 3 most delayed flights on 2000-01-16 from AUS to ORD
# MAGIC
# MAGIC Collect the results into a list and display the output as the following.
# MAGIC ```
# MAGIC     AA flight delayed by 5.0 minutes
# MAGIC     AA flight delayed by 2.0 minutes
# MAGIC     UA flight delayed by 2.0 minutes
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import expr

top_3_df = (
    flight_time_df.where("FL_DATE = '2000-01-16' and origin = 'AUS' and dest = 'ORD'")
        .withColumn("delayed_arrival", expr("arr_time - crs_arr_time"))
        .orderBy("delayed_arrival", ascending=False)
        .limit(3)
)

top_3_list_of_rows = top_3_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Make a list of dictionary from a list of rows

# COMMAND ----------

top_3_list = [row.asDict() for row in top_3_list_of_rows]

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Print the result in desired format

# COMMAND ----------

for i in top_3_list:
    print(i["OP_CARRIER"] 
          + " flight delayed by "
          + str(i["delayed_arrival"].total_seconds()/60)
          + " minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Find the third most delayed flights on 2000-01-16 from AUS to ORD

# COMMAND ----------

top_3rd_df = (
    flight_time_df.where("FL_DATE = '2000-01-16' and origin = 'AUS' and dest = 'ORD'")
        .withColumn("delayed_arrival", expr("arr_time - crs_arr_time"))
        .orderBy("delayed_arrival", ascending=False)
        .limit(3)
        .offset(2)
)

top_3rd_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC
# MAGIC