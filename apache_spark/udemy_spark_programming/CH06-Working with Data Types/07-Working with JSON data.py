# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Requirement
# MAGIC Read data from students_online.json file and load into online_students table.

# COMMAND ----------

online_students_schema = """
    ID string, FirstName string, LastName string,
    Address struct<AddressLine1 string, AddressLine2 string, City string, State string, Country string, Pin string>,
    Skills array<struct<Skill string, YearsOfExperience string>>,
    Contacts map<string, string>
    """

online_students_df = (
    spark.read.format("json")
        .schema(online_students_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/students_online.json")
)

#online_students_df.display()
online_students_df.write.mode("overwrite").saveAsTable("dev.spark_db.online_students")

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Requirement
# MAGIC Perform the following analysis
# MAGIC 1. What is country wise student count.
# MAGIC 2. Find all students with more than 1 years of Spark knowledge
# MAGIC 3. Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 What is country wise student count.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Address.Country, count(*) as count
# MAGIC from dev.spark_db.online_students
# MAGIC group by Address.Country

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Find all students with more than 1 years of Spark knowledge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with online_students_skills(
# MAGIC   select id, firstname, LastName, explode(Skills) as skills
# MAGIC   from dev.spark_db.online_students)
# MAGIC select id, firstname, lastname, skills.*
# MAGIC from online_students_skills
# MAGIC where skills.skill like "%Spark%" and skills.YearsOfExperience > 1

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Find all students who didn't provide phone or whatsapp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select id, FirstName, LastName, Contacts['email'] as email
# MAGIC from dev.spark_db.online_students
# MAGIC where Contacts['phone'] is null and Contacts['whatsapp'] is null

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC