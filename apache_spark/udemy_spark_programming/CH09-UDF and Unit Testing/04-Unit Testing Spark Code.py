# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Unit Testing Spark code
# MAGIC 1. What do we unit test?
# MAGIC ####How do you want to test?
# MAGIC 1. Design for unit test
# MAGIC     * Reusable Modular code (Refactor your code if needed)
# MAGIC 2. Prepare test data
# MAGIC 3. Write unit tests
# MAGIC 4. Run unit tests

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How do you write Spark code?
# MAGIC You have booking summary data file booking-summary.csv.\
# MAGIC Read the data from the file and prepare top 3 revenue dates for guest and members as shown below.
# MAGIC ```
# MAGIC +---------+------------+-------+
# MAGIC |booked_by|booking_date|revenue|
# MAGIC +---------+------------+-------+
# MAGIC |    Guest|  2022-07-24| 1105.0|
# MAGIC |    Guest|  2022-07-27|  990.0|
# MAGIC |    Guest|  2022-07-30|  986.5|
# MAGIC |   Member|  2022-07-25|  626.0|
# MAGIC |   Member|  2022-07-31|  486.0|
# MAGIC |   Member|  2022-07-26|  455.0|
# MAGIC +---------+------------+-------+
# MAGIC ```

# COMMAND ----------

file_schema = "booked_by string, booking_date date, revenue double"

summary_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(file_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/booking-summary.csv")
)

#summary_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

window_spec = (
    Window.partitionBy("booked_by")
        .orderBy(col("revenue").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = (
    summary_df
        .withColumn("rank", rank().over(window_spec))
        .where("rank <= 3")
        .drop("rank")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Refactoring or Follow Best Practices for Coding
# MAGIC 1. Modular and Reusable Code
# MAGIC 2. Object Oriented Approach / Functional Approach

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC