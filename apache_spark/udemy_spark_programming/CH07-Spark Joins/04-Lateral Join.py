# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lateral Join
# MAGIC Lateral join allows to query right dataframe for each row of the left dataframe.\
# MAGIC Lateral joins are especially useful when:
# MAGIC 1. You need per-parent Top-N child rows
# MAGIC 2. You want to invoke TVFs with arguments derived from each row
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Find the most recent booking for each member
# MAGIC ```
# MAGIC +---------+----------+---------+---------------+-------------------+-----+
# MAGIC |member_id|first_name|last_name|  facility_name|         start_time|slots|
# MAGIC +---------+----------+---------+---------------+-------------------+-----+
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, expr

members_df = (
    spark.table("dev.spark_db.members")
        .filter("member_id > 0")
        .select("member_id", "first_name", "last_name")
        .alias("m")
)

bookings_df = spark.table("dev.spark_db.bookings").alias("b")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

latest_member_bookings_df = (
    members_df.lateralJoin(
        bookings_df.where("b.member_id == m.member_id")
            .orderBy(col("start_time").desc())
            .limit(1), None, "left")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.start_time", "b.slots")
).alias("mb")

result_df = (
    latest_member_bookings_df.join(facilities_df, on=expr("mb.facility_id == f.facility_id"), how="left")
    .select("mb.member_id", "mb.first_name", "mb.last_name", "f.facility_name", "mb.start_time", "mb.slots")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Find all students with more than 1 years of Spark knowledge from offline_var_students

# COMMAND ----------

students_df = spark.table("dev.spark_db.offline_var_students").alias("s")

result_df = (
    students_df.lateralJoin(spark.tvf.variant_explode("s.skills")
                            .selectExpr("cast(value:Skill as string) as skill", "cast(value:YearsOfExperience as int) as experience"))
                .where("skill like '%Spark%' and experience > 1")
                .select("id", "first_name", "last_name", "skill", "experience")
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