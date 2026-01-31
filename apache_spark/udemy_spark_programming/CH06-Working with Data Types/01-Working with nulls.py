# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Handling Null values
# MAGIC 1. Equality check
# MAGIC 2. Null in expressions
# MAGIC 3. [Conditional functions for Null](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#conditional-functions)
# MAGIC 4. Null in aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Create a data frame to demo some scenarios

# COMMAND ----------

person_list = [(100, "Prashant", 30),
             (101, "David", None),
             (102, "Sushant",None),
             (103, "Abdul", 45),
             (104, "Shruti", 28)]
             
person_df = spark.createDataFrame(person_list).toDF("id", "name", "age")
display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. How null equality is executed.

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Find all records where age is 28.\
# MAGIC 2.1 Find all records where age is not given or unknown.

# COMMAND ----------

person_df.where("age == null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Create a boolean column to investigate

# COMMAND ----------

from pyspark.sql.functions import expr

person_df.withColumn("age_null_expr", expr("age IS null")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Use case for null equality
# MAGIC
# MAGIC Select only those persons having a valid age information

# COMMAND ----------

person_df.where("age IS null").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. How operators work on null values

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1 Check the result of > operator on null values

# COMMAND ----------

person_df.withColumn("age_gt_29", expr("age > 29")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 How the comperison operator behaves in answering business questions.
# MAGIC
# MAGIC Find all employees where age is greater than 29

# COMMAND ----------

person_df.where("age > 29").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.3 How mathametical operators work on null.\
# MAGIC Calculate experience for every employee using the following formula.\
# MAGIC experience = age - 23
# MAGIC

# COMMAND ----------

person_df.withColumn("experience", expr("age - 23")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.4. Calculate the experience knowing that age could be null.\
# MAGIC If age is null then assume 23 years for experience calculation.

# COMMAND ----------

person_df.withColumn("experience", expr("nvl(age, 23) - 23")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. How aggregates work on null values

# COMMAND ----------

# MAGIC %md
# MAGIC 4.1 What is the average age?

# COMMAND ----------

person_df.selectExpr("avg(age)").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 What if we filter out null values before aggregation

# COMMAND ----------

person_df.where("age is not null").selectExpr("avg(age)").display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>