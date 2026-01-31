# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create a Dataframe using a connector

# COMMAND ----------

file_df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
)

# COMMAND ----------

json_file_df = (
    spark.read.format("json")
         .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Create a Dataframe reading a Spark table

# COMMAND ----------

table_df = spark.table("dev.spark_db.sf_fire_calls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Create a Dataframe reading the result of a SQL query

# COMMAND ----------

table_name  = "dev.spark_db.sf_fire_calls"

sql_df = spark.sql(f"""select * 
                   from {table_name} 
                   limit 5""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Create a dataframe from a Python list

# COMMAND ----------

from datetime import datetime, date

data_list_schema = 'id long, name string, joining_date date, salary double, created_at timestamp'

data_list = [(1, "Prashant", date(2018, 1, 1), 924.0, datetime(2022, 1, 1, 9, 0)),
             (2, "Sushant", date(201, 2, 1), 1260.50, datetime(2022, 1, 2, 11, 0)),
             (3, "David", date(2022, 3, 1), 765.0, datetime(2022, 1, 3, 10, 0))]

list_df = spark.createDataFrame(data_list, data_list_schema) 


# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Create a single column dataframe from a range

# COMMAND ----------

range_df = spark.range(1000, 1010, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC