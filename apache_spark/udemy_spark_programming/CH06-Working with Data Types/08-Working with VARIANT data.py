# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Requirement
# MAGIC Read data from students_offline.csv file and load into offline_students_raw table.

# COMMAND ----------

offline_students_schema = "id string, first_name string, last_name string, address string, skills string, contacts string"

offline_students_raw_df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(offline_students_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/students_offline.csv")
)

#offline_students_raw_df.display()
offline_students_raw_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev.spark_db.offline_students_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Requirement
# MAGIC Prepare an offline_var_students table which is ready for analysis
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import parse_json

offline_students_df = (
    offline_students_raw_df.withColumns({
        "address": parse_json("address"),
        "skills": parse_json("skills"),
        "contacts": parse_json("contacts")
    })
)

#offline_students_df.display()
offline_students_df.write.mode("overwrite").saveAsTable("dev.spark_db.offline_var_students")

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Requirement
# MAGIC Perform the following analysis
# MAGIC 1. What is country wise student count.
# MAGIC 2. Find all students with more than 1 years of Spark knowledge
# MAGIC 3. Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 What is country wise student count.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Variant object element names are case sensitive
# MAGIC
# MAGIC select cast(address:Country as string), count(*) as count
# MAGIC from dev.spark_db.offline_var_students
# MAGIC group by cast(address:Country as string)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Find all students with more than 1 years of Spark knowledge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with offline_students_skills(
# MAGIC   select id, first_name, last_name, cast(value:Skill as string), cast(value:YearsOfExperience as int)
# MAGIC   from dev.spark_db.offline_var_students, lateral variant_explode_outer(skills)
# MAGIC )
# MAGIC select *
# MAGIC from offline_students_skills
# MAGIC where skill like "%Spark%" and yearsofexperience>1

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id, first_name, last_name, contacts:email
# MAGIC from dev.spark_db.offline_var_students
# MAGIC where contacts:phone is null and contacts:whatsapp is null

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC