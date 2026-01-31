# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Introduction to Aggregation
# MAGIC Aggregation in Spark are implemented using functions
# MAGIC
# MAGIC #####Comonly used aggregate functions
# MAGIC 1. count(*), count(expr), count(DISTINCT expr)
# MAGIC 2. min(expr), max(expr), avg(expr), sum(expr)
# MAGIC
# MAGIC #####Types of Aggregation
# MAGIC 1. Simple Aggregation
# MAGIC 2. Grouped Aggregation
# MAGIC 3. Multilevel Aggregation
# MAGIC 4. Window Aggregation
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement - Analysis Data Set
# MAGIC
# MAGIC Prepare club bookings dataset for analysis
# MAGIC ```
# MAGIC +----------+------------+---------------+-------------------+--------------+
# MAGIC |booking_id| member_name|  facility_name|         start_time|booking_amount|
# MAGIC +----------+------------+---------------+-------------------+--------------+
# MAGIC ```

# COMMAND ----------

bookings_df = spark.table("dev.spark_db.bookings")
facilities_df = spark.table("dev.spark_db.facilities")
members_df = spark.table("dev.spark_db.members")

club_bookings_df = (
    bookings_df.join(facilities_df, "facility_id")
            .join(members_df, "member_id", "left")
            .selectExpr("booking_id",
                        "case when member_id==0 then 'Guest Member' else concat_ws(' ', first_name, last_name) end as member_name",
                        "facility_name","start_time",
                        "case when member_id == 0 then slots * guest_cost else slots * member_cost end as booking_amount")            
)

club_bookings_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Calculate total earnings and average booking value.

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Using sql like expressions

# COMMAND ----------

result_df = (
    club_bookings_df.selectExpr("sum(booking_amount) as total_earning",
                                "avg(booking_amount) as avg_booking_value")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Using column expressions

# COMMAND ----------

from pyspark.sql.functions import sum, avg

result_df = (
    club_bookings_df.select(sum("booking_amount").alias("total_earning"),
                            avg("booking_amount").alias("avg_booking_value"))
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.3 Using aggregate transformation

# COMMAND ----------

from pyspark.sql.functions import sum, avg

result_df = (
    club_bookings_df.agg(sum("booking_amount").alias("total_earning"),
                         avg("booking_amount").alias("avg_booking_value"))
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC