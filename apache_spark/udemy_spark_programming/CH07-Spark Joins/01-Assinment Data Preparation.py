# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join types in Apache Spark
# MAGIC 1. Inner Join - Return only the rows that have matching values on both sides
# MAGIC 2. Outer Joins
# MAGIC     * Left - Returns all rows from the left side and matching rows from the right side
# MAGIC     * Right - Returns all rows from the right side and matching rows from the left side
# MAGIC     * Full - Return all matching and non-matching rows from both sides
# MAGIC 3. Natural Join - Automatically create join criteria on the same column names (Applies to Inner and Outer Joins)
# MAGIC 4. Cross Join - Join without any join criteria (all possible combinations)
# MAGIC 5. Self Join - Join a table with itself (Applies to Inner, Outer, and Cross Joins)
# MAGIC 6. Semi Join - Take records from the left side when it matches with the right side
# MAGIC 7. Anti Join - Take records from the left side when it doesn not match with the right side
# MAGIC 8. Lateral Join - Each row from the driving table is used to subquery the derived table

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Load data from files and create the following tables.
# MAGIC 1. facilities -> facilities.csv
# MAGIC 2. members -> members.csv
# MAGIC 3. bookings -> bookings.csv
# MAGIC
# MAGIC Choose appropriate data types to best represent the data fields.

# COMMAND ----------

# MAGIC %md
# MAGIC <br>
# MAGIC
# MAGIC <img src ='https://learningjournal.github.io/pub-resources/images/club_data_model.jpg' alt="Club Data Model" style="width: 300px">

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Define schema

# COMMAND ----------

facilities_schema = """facility_id INT, facility_name STRING, member_cost DOUBLE, guest_cost DOUBLE, 
    initial_outlay DOUBLE, monthly_maintainance DOUBLE"""

members_schema = """member_id INT, last_name STRING, first_name STRING, address STRING, zip_code STRING, 
    telephone STRING, recommended_by STRING, joining_date DATE"""

bookings_schema = "booking_id INT, facility_id INT, member_id INT, start_time TIMESTAMP, slots INT"

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Load facilities table

# COMMAND ----------

facilities_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(facilities_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/facilities.csv")
)

facilities_df.write.mode("overwrite").saveAsTable("dev.spark_db.facilities")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Load members table

# COMMAND ----------

members_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(members_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/members.csv")
)

members_df.write.mode("overwrite").saveAsTable("dev.spark_db.members")

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Load bookings table

# COMMAND ----------

bookings_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(bookings_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/bookings.csv")
)

bookings_df.write.mode("overwrite").saveAsTable("dev.spark_db.bookings")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC