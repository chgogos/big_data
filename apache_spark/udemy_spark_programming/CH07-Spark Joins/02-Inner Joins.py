# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Prepare a facility bookings reporting dataset as the following.
# MAGIC ```
# MAGIC member_id | first_name | last_name | facility_id | slots | start_time
# MAGIC ---------------------------------------------------------------------------
# MAGIC ```
# MAGIC The report must meet the following criteria.
# MAGIC 1. Facility bookings made by a person whose last name is Smith
# MAGIC 2. He has booked more than 5 slots in a single booking
# MAGIC 3. Report should be sorted by first name of the member in ascending order and number of slots in descending order
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Try answering with SQL
# MAGIC
# MAGIC * Join Expression
# MAGIC * Join type
# MAGIC * Column name ambiguity

# COMMAND ----------

# MAGIC %sql
# MAGIC select m.member_id, first_name, last_name, facility_id, slots, start_time
# MAGIC from dev.spark_db.bookings as b inner join dev.spark_db.members as m on m.member_id = b.member_id
# MAGIC where m.last_name = "Smith" and b.slots > 5
# MAGIC order by m.first_name asc, b.slots desc

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Try answering with dataframe api
# MAGIC * Join Expression
# MAGIC * Join type
# MAGIC * Column name ambiguity

# COMMAND ----------

from pyspark.sql.functions import expr, col

members_df = spark.table("dev.spark_db.members").alias("m")
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

#join_expr = expr("m.menber_id == b.member_id")
join_expr = col("m.member_id") == col("b.member_id")

reports_df = (
    members_df.join(bookings_df, join_expr, "inner")
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.slots", "b.start_time")
        .orderBy("m.first_name", col("b.slots").desc())
)

reports_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Show me a facility bookings report as the following.
# MAGIC ```
# MAGIC member_id | first_name | last_name | facility_name | slots | booking_amount | start_time
# MAGIC --------------------------------------------------------------------------------------------
# MAGIC ```
# MAGIC The report must meet the following criteria.
# MAGIC 1. Facility bookings made by a person whose last name is Smith
# MAGIC 2. He has booked more than 5 slots in a single booking
# MAGIC 3. Report should be sorted by first name of the member in ascending order and booking amount in descending order

# COMMAND ----------

from pyspark.sql.functions import col, expr

bookings_df = spark.table("dev.spark_db.bookings").alias("b")
members_df = spark.table("dev.spark_db.members").alias("m")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

report_df = (
    bookings_df
        .join(members_df, expr("b.member_id == m.member_id"), "inner")
        .join(facilities_df, col("b.facility_id") == col("f.facility_id"), "inner")
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .selectExpr("m.member_id", "m.first_name", "m.last_name", "f.facility_name", "b.slots",
            "b.slots * f.member_cost as booking_amount", "b.start_time")
        .orderBy(col("m.first_name").asc(),
                 col("booking_amount").desc())
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC