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

offline_students_schema = "ID string, FirstName string, LastName string, Address string, Skills string, Contacts string"

offline_students_raw_df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(offline_students_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/students_offline.csv")
)

#offline_students_raw_df.display()
offline_students_raw_df.write.mode("overwrite").saveAsTable("dev.spark_db.offline_students_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Analysis Requirement
# MAGIC We want to know country wise student count.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with offline_students(
# MAGIC   select id, from_json(address,
# MAGIC       """struct<AddressLine1 string,
# MAGIC         AddressLine2 string,
# MAGIC         City string,
# MAGIC         Country string,
# MAGIC         Pin string,
# MAGIC         State string>
# MAGIC       """) as address
# MAGIC   from dev.spark_db.offline_students_raw
# MAGIC )
# MAGIC select address.country, count(*) as count
# MAGIC from offline_students
# MAGIC group by address.country

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Requirement
# MAGIC Prepare an offline_students table which is ready for analysis
# MAGIC
# MAGIC Complex Data Types in Spark
# MAGIC 1. Struct
# MAGIC 2. Array
# MAGIC 3. Map

# COMMAND ----------

from pyspark.sql.functions import from_json

address_schema = "struct<AddressLine1 string, AddressLine2 string, City string, Country string, Pin string, State string>"
skills_schema = "array<struct<Skill string, YearsOfExperience string>>"
contacts_schema = "map<string, string>"

offline_students_df = (
    offline_students_raw_df.withColumns({
        "address": from_json("address", address_schema),
        "skills": from_json("skills", skills_schema),
        "contacts": from_json("contacts", contacts_schema)
    })
)

#offline_students_df.display()
offline_students_df.write.mode("overwrite").saveAsTable("dev.spark_db.offline_students")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Requirement
# MAGIC Perform the following analysis
# MAGIC 1. What is country wise student count.
# MAGIC 2. Find all students with more than 1 years of Spark knowledge
# MAGIC 3. Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %md
# MAGIC 4.1 What is country wise student count.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select address.Country, count(*) as count
# MAGIC from dev.spark_db.offline_students
# MAGIC group by address.Country

# COMMAND ----------

# MAGIC %md
# MAGIC 4.2 Find all students with more than 1 years of Spark knowledge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with offline_students_skills(
# MAGIC    select id, FirstName, LastName, explode(skills) as skills
# MAGIC    from dev.spark_db.offline_students
# MAGIC )
# MAGIC select id, firstname, lastname, skills.*
# MAGIC from offline_students_skills
# MAGIC where skills.Skill like "%Spark%" and skills.YearsOfExperience > 1

# COMMAND ----------

# MAGIC %md
# MAGIC 4.3 Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ID, FirstName, LastName, contacts['email']
# MAGIC from dev.spark_db.offline_students
# MAGIC where contacts['phone'] is null and contacts['whatsapp'] is null

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC