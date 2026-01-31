# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Other Types of joins
# MAGIC 1. Natural Join - Automatically create join criteria on the same column names (Applies to Inner and Outer Joins)
# MAGIC 2. Cross Join - Join without any join criteria (all possible combinations)
# MAGIC 3. Self Join - Join a table with itself (Applies to Inner, Outer, and Cross Joins)
# MAGIC 4. Semi Join - Take records from the left side when it matches with the right side (Correlated EXISTS)
# MAGIC 5. Anti Join - Take records from the left side when it doesn not match with the right side (Correlated NOT EXISTS)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Show me a facility bookings report as the following. (Prefer Natural Join)
# MAGIC ```
# MAGIC member_id | first_name | last_name | facility_name | slots | booking_amount | start_time
# MAGIC --------------------------------------------------------------------------------------------
# MAGIC ```
# MAGIC The report must meet the following criteria.
# MAGIC 1. Facility bookings made by a person whose last name is Smith
# MAGIC 2. He has booked more than 5 slots in a single booking
# MAGIC 3. Report should be sorted by first name of the member in ascending order and booking amount in descending order

# COMMAND ----------

from pyspark.sql.functions import col

bookings_df = spark.table("dev.spark_db.bookings").filter("slots > 5")
members_df = spark.table("dev.spark_db.members").filter("last_name == 'Smith'")
facilities_df = spark.table("dev.spark_db.facilities")

bmf_df = bookings_df.join(members_df, ["member_id"], "inner").join(facilities_df, "facility_id")

report_df = (
    bmf_df.selectExpr("member_id", "first_name", "last_name", "facility_name", "slots",
                "slots * member_cost as booking_amount", "start_time")
        .orderBy("first_name", col("booking_amount").desc())
)

display(report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Prepare a member bookings report as the following (Prefer Natural Join)
# MAGIC ```
# MAGIC booking_id | facility_name | slots | first_name | last_name | address
# MAGIC ```
# MAGIC Ensure the following
# MAGIC 1. Consider only regular memebrs (not guest) and direct members(not recomended by any other member)
# MAGIC 2. Consider only bookings for more than 8 hours
# MAGIC 3. Ensure all regular and direct members are listed even if they have no 8 hour bookings
# MAGIC 4. Ensure all 8 hour bookings are listed even if they are not made by regular and direct members
# MAGIC 5. Sort the report by slots and first name in ascending order

# COMMAND ----------

members_df = spark.table("dev.spark_db.members").filter("member_id != 0  and recommended_by is null")
bookings_df = spark.table("dev.spark_db.bookings").filter("slots > 8")
facilities_df = spark.table("dev.spark_db.facilities")

joined_df = (
    members_df.join(bookings_df, "member_id", "full")
            .join(facilities_df, "facility_id", "left")
)

report_df = (
    joined_df.select("booking_id", "facility_name", "slots", "first_name", "last_name", "address")
        .orderBy("slots", "first_name")
)

display(report_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. How many bookings are possible when each member is booking a facility exactly once in a month?\
# MAGIC Show all possible combinations

# COMMAND ----------

members_df = spark.table("dev.spark_db.members").filter("member_id > 0")
facilities_df = spark.table("dev.spark_db.facilities")

report_df = (
    members_df.crossJoin(facilities_df)
        .select("first_name", "last_name", "facility_name")
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Prepare a report for members and who recomended them as the following
# MAGIC ```
# MAGIC member_id | Member Name | Recommended By
# MAGIC --------------------------------------------
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import expr, concat_ws

members_df = spark.table("dev.spark_db.members")

report_df = (
    members_df.alias("m")
        .join(members_df.alias("r"), expr("m.recommended_by==r.member_id"), "inner")
        .select("m.member_id",
                concat_ws(" ", "m.first_name", "m.last_name").alias("Member Name"),
                concat_ws(" ", "r.first_name", "r.last_name").alias("Recommended By"))
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q5. Prepare a list of members who made at least one booking. (Use SEMI Join)
# MAGIC ```
# MAGIC member_id | first_name | last_name | address
# MAGIC -----------------------------------------------
# MAGIC ```

# COMMAND ----------

members_df = spark.table("dev.spark_db.members").filter("member_id > 0").alias("m")
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

report_df = (
    members_df.join(bookings_df, expr("m.member_id == b.member_id"), "left_semi")
        .select("member_id", "first_name", "last_name", "address")
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. Prepare a list of members who never made any bookings. (Use ANTI Join)
# MAGIC ```
# MAGIC member_id | first_name | last_name | address
# MAGIC -----------------------------------------------
# MAGIC ```

# COMMAND ----------

members_df = spark.table("dev.spark_db.members").filter("member_id > 0").alias("m")
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

report_df = (
    members_df.join(bookings_df, expr("m.member_id == b.member_id"), "left_anti")
        .select("member_id", "first_name", "last_name", "address")
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