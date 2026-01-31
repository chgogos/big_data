# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement:
# MAGIC We have colleced fire calls data file sf-fire-calls.csv.
# MAGIC 1. Read the data file
# MAGIC 2. Load it into a table for analysis
# MAGIC 3. Verify all 175296 records are loaded correctly
# MAGIC 4. The table is predefined as below

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.sf_fire_calls (
# MAGIC   CallNumber INT, UnitID STRING, IncidentNumber INT, CallType STRING, CallDate DATE,
# MAGIC   WatchDate DATE, CallFinalDisposition STRING, AvailableDtTm TIMESTAMP, Address STRING,
# MAGIC   City STRING, Zipcode STRING, Battalion STRING, StationArea STRING, Box STRING,
# MAGIC   OriginalPriority STRING, Priority STRING, FinalPriority STRING, ALSUnit BOOLEAN,
# MAGIC   CallTypeGroup STRING, NumAlarms INT, UnitType STRING, UnitSequenceInCallDispatch INT,
# MAGIC   FirePreventionDistrict STRING, SupervisorDistrict STRING, Neighborhood STRING,
# MAGIC   Location STRING, RowID STRING, Delay DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC ####Solution approach

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Check the data file structure.
# MAGIC 2. Read the data file and create a dataframe

# COMMAND ----------

raw_fire_df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Count the records(count)
# MAGIC 4. Check the dataframe for potential problems (display/show)
# MAGIC 5. Verify dataframe schema with the target table (printSchema)
# MAGIC 6. Transform the dataframe to match target table structure (withColumns, to_date, to_timestamp, cast)

# COMMAND ----------

raw_fire_df.count()

# COMMAND ----------

raw_fire_df.display()

# COMMAND ----------

raw_fire_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, expr

fire_df = raw_fire_df.withColumns({
        "AvailableDtTm": to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"),
        "Zipcode": expr("cast(Zipcode as string)"),
        "FinalPriority": expr("cast(FinalPriority as string)")
    })

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Save the final dataframe into target table

# COMMAND ----------

fire_df.write.mode("overwrite").saveAsTable("dev.spark_db.sf_fire_calls")

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Verify the table count

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from dev.spark_db.sf_fire_calls

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC