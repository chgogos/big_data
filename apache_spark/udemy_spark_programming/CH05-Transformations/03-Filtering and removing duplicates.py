# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement
# MAGIC Create a test Dataframe as shown below
# MAGIC ```
# MAGIC   +---+---------+-----------+--------+
# MAGIC   | id|   source|destination|distance|
# MAGIC   +---+---------+-----------+--------+
# MAGIC   |101|   Mumbai|        Goa|     587|
# MAGIC   |102|   Mumbai|  Bangalore|     985|
# MAGIC   |102|   Mumbai|  Bangalore|     985|
# MAGIC   |103|    Delhi|    Chennai|    2208|
# MAGIC   |104|    Delhi|    Chennai|    2208|
# MAGIC   |105|Bangalore|    Kolkata|    1868|
# MAGIC   |105|Bangalore|    Kolkata|    1865|
# MAGIC   +---+---------+-----------+--------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Select records that satisfy the follwing criteria
# MAGIC 1. Source is Mumbai and destination is Bangalore
# MAGIC
# MAGIC Show the following approaches
# MAGIC 1. Using a single filter
# MAGIC 2. Using two filters
# MAGIC 3. Using a col function expression
# MAGIC 4. Using a dataframe qualifier expression

# COMMAND ----------

data_schema = "id int, source string, destination string, distance int"

data_list = [(101, "Mumbai", "Goa", 587),
             (102, "Mumbai", "Bangalore", 985),
             (102, "Mumbai", "Bangalore", 985),
             (103, "Delhi", "Chennai", 2208),
             (104, "Delhi", "Chennai", 2208),
             (105, "Bangalore", "Kolkata", 1868),
             (105, "Bangalore", "Kolkata", 1865)
             ]

df = spark.createDataFrame(data=data_list, schema=data_schema)
display(df)             

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Using a single filter

# COMMAND ----------

df.filter("source = 'Mumbai' and destination='Bangalore'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Using two filters

# COMMAND ----------

df.filter("source = 'Mumbai'").filter("destination = 'Bangalore'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Using a col function expression

# COMMAND ----------

from pyspark.sql.functions import col

df.filter((col("source") == 'Mumbai') & (col("destination") == 'Bangalore')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.4 Using a dataframe qualifier expression

# COMMAND ----------

df.filter((df.source == 'Mumbai') & (df.destination == 'Bangalore')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Remove duplicate records from your dataframe
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1 what do you mean by duplicate?
# MAGIC

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 Remove all the duplictes based on all the column values
# MAGIC

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.3 Remove duplicates based on the specified column values

# COMMAND ----------

df.dropDuplicates(["id", "source", "destination"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC
# MAGIC