# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. List all bookings made by a person named Darren Smith as the following.
# MAGIC ```
# MAGIC member_id | first_name | last_name | address | facility_id | slots
# MAGIC ---------------------------------------------------------------------
# MAGIC ```
# MAGIC
# MAGIC Ensure the following
# MAGIC 1. Show the details of all persons named Darren Smith even if they have not made any bookings
# MAGIC 2. Sort the result by number of slots (highers first)
# MAGIC 3. List the person with no bookings at the top

# COMMAND ----------

from pyspark.sql.functions import expr, col

members_df = spark.table("dev.spark_db.members").alias("m")
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

result_df = (
    members_df.join(bookings_df, expr("m.member_id=b.member_id"), "left")
        .where("m.first_name == 'Darren' and m.last_name == 'Smith'")
        .select("m.member_id", "m.first_name", "m.last_name", "m.address", "b.facility_id", "b.slots")
        .orderBy(col("b.slots").desc_nulls_first())
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Show me a bookings report for Darren Smith as the following.
# MAGIC
# MAGIC ```
# MAGIC facility_name | slots | booking_amount | start_time | member_id | member_name | telephone | address
# MAGIC ------------------------------------------------------------------------------------------------------
# MAGIC ```
# MAGIC The report must meet the following criteria.
# MAGIC
# MAGIC 1. Show the details of all persons named Darren Smith even if they have not made any bookings
# MAGIC 2. Sort the result by number of slots (highers first)
# MAGIC 3. List the person with no bookings at the top

# COMMAND ----------

from pyspark.sql.functions import expr, col

members_df = (
    spark.table("dev.spark_db.members")
        .where("first_name='Darren' and last_name='Smith'")
)

bookings_df = spark.table("dev.spark_db.bookings")
facilities_df = spark.table("dev.spark_db.facilities")

joined_df = (
    members_df.alias("m")
        .join(bookings_df.alias("b"), expr("m.member_id = b.member_id"), "left")
        .join(facilities_df.alias("f"), expr("b.facility_id=f.facility_id"), "left")
)

results_df = (
    joined_df.selectExpr(
        "f.facility_name", "b.slots",
        "b.slots * f.member_cost as booking_amount",
        "b.start_time", "b.member_id",
        "concat_ws(' ', m.first_name, m.last_name)  as member_name",
        "m.telephone", "m.address"
    ).orderBy(col("slots").desc_nulls_first())    
)

results_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Prepare a facility booking report as the following
# MAGIC ```
# MAGIC facility_name | member_cost | gest_cost | start_time | slots
# MAGIC ---------------------------------------------------------------
# MAGIC ```
# MAGIC Ensure the following
# MAGIC 1. All club facilities must be listed in the report
# MAGIC 2. Consider only bookings for more than 10 slots

# COMMAND ----------

from pyspark.sql.functions import expr

facilities_df = spark.table("dev.spark_db.facilities")
bookings_df = spark.table("dev.spark_db.bookings").filter("slots > 10")

result_df = (
    bookings_df.join(facilities_df, bookings_df.facility_id == facilities_df.facility_id, "right")
             .select(facilities_df.facility_name,
                    facilities_df.member_cost,
                    facilities_df.guest_cost,
                    bookings_df.start_time,
                    bookings_df.slots)
)

result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Prepare a member bookings report as the following
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

from pyspark.sql.functions import expr

members_df = (
    spark.table("dev.spark_db.members")
        .filter("member_id != 0 and recommended_by is null")
        .alias("m")
)

bookings_df = (
    spark.table("dev.spark_db.bookings")
        .filter("slots > 8")
        .alias("b")
)

facilities_df = spark.table("dev.spark_db.facilities").alias("f")

full_join_df = members_df.join(bookings_df, expr("m.member_id == b.member_id"), "full")

result_df = (
    full_join_df.join(facilities_df, expr("b.facility_id == f.facility_id"), "left")
    .select("b.booking_id","f.facility_name","b.slots","m.first_name","m.last_name","m.address")
    .orderBy(expr("b.slots").asc_nulls_last(), expr("m.first_name").asc_nulls_last())
)

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC