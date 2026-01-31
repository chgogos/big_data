# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

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
# MAGIC Q1. Who are the top 5 members by total booking amount?
# MAGIC
# MAGIC Prepare a report as the following.
# MAGIC ```
# MAGIC member_name     | total_booking_amount
# MAGIC ---------------------------------------
# MAGIC Tim Rownam      | 6480
# MAGIC Tim Boothe      | 3644
# MAGIC Gerald Butters  | 3343
# MAGIC Burton Tracy    | 2953
# MAGIC David Jones     | 2651
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Try aggregation using select or selectExpr

# COMMAND ----------

result_df = (
    club_bookings_df.where("member_name != 'Guest Member'")
            .groupBy("member_name")
            .selectExpr("member_name",
                        "sum(booking_amount) as total_booking_amount")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Try using agg() transformation

# COMMAND ----------

from pyspark.sql.functions import expr, col

result_df = (
    club_bookings_df.where("member_name != 'Guest Member'")
            .groupBy("member_name")
            .agg(expr("sum(booking_amount) as total_booking_amount"))
            .orderBy(col("total_booking_amount").desc())
            .limit(5)
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Who are the members having total booking amount > 2500?

# COMMAND ----------

from pyspark.sql.functions import expr, col

result_df = (
    club_bookings_df.where("member_name != 'Guest Member'")
            .groupBy("member_name")
            .agg(expr("sum(booking_amount) as total_booking_amount"))
            .where("total_booking_amount > 2500")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Find member wise facility bookings for more than 2500?
# MAGIC ```
# MAGIC +-----------+--------------+--------------------+
# MAGIC |member_name| facility_name|total_booking_amount|
# MAGIC +-----------+--------------+--------------------+
# MAGIC | Tim Boothe|Massage Room 1|              2660.0|
# MAGIC | Tim Rownam|Massage Room 1|              6160.0|
# MAGIC +-----------+--------------+--------------------+
# MAGIC ```
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col

result_df = (
    club_bookings_df.where("member_name != 'Guest Member'")
            .groupBy("member_name", "facility_name")
            .agg(expr("sum(booking_amount) as total_booking_amount"))
            .where("total_booking_amount > 2500")
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