# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark UDF
# MAGIC 1. Scalar Python UDF
# MAGIC 2. Pandas Vectorized UDF
# MAGIC 3. UDTF
# MAGIC ####Scalar Python UDF
# MAGIC 1. User-defined functions 
# MAGIC 2. Take or return Python objects
# MAGIC 3. Operate one row at a time
# MAGIC 4. Serialized/Deserialized by pickle or Arrow 

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How to define and use a Python UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Define a UDF\
# MAGIC Create a UDF with the following functionality
# MAGIC * Input -> member_id
# MAGIC * Output -> Guest/Member

# COMMAND ----------

from pyspark.sql.functions import udf

@udf(returnType="string", useArrow=True)
def member_type_udf(id: str):
    return 'Guest' if id==0 else 'Member'

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Use a UDF in Dataframe Transformations

# COMMAND ----------

member_df = spark.table("dev.spark_db.members")

result_df = member_df.withColumn("member_type", member_type_udf("member_id"))

result_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.3. Register UDF for use in Spark SQL

# COMMAND ----------

spark.udf.register("member_type_udf", member_type_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.4 UDF from Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *, member_type_udf(member_id) as member_type
# MAGIC from dev.spark_db.members
# MAGIC limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC 1.5 UDF in Expressions

# COMMAND ----------

from pyspark.sql.functions import expr

member_df = spark.table("dev.spark_db.members")

result_df = member_df.withColumn("member_type", expr("member_type_udf(member_id)"))

result_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC