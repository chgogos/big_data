# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Working with numbers
# MAGIC 1. Using mathematical expressions
# MAGIC 2. [Using mathematical functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#mathematical-functions)
# MAGIC 3. [Using aggregate functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Load invoices data to a dataframe

# COMMAND ----------

retail_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/dev/spark_db/datasets/spark_programming/data/invoices.csv")
)

display(retail_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Calculate total_value for each invoice line item
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Simple approach of creating expressions

# COMMAND ----------

from pyspark.sql.functions import expr

retail_df.withColumn("total_value", expr("round(quantity * unitprice, 2)")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Using column expressions

# COMMAND ----------

from pyspark.sql.functions import col, round

retail_df.withColumn("total_value", round(col("quantity") * col("unitprice"), 2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Using expression variable - 1
# MAGIC

# COMMAND ----------

total_value_expr_1 = round(col("quantity") * col("unitprice"), 2)

retail_df.withColumn("total_value", total_value_expr_1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.4 Using expression variable - 2
# MAGIC

# COMMAND ----------

total_value_expr_2 = expr("round(quantity * unitprice, 2)")

retail_df.withColumn("total_value", total_value_expr_2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Perform the following exploratory analysis on invoices data
# MAGIC 1. Can we make invoice numbers a numeric field?
# MAGIC 2. Analyize quantity to identify potentially invalid records
# MAGIC 3. Analyze unit price to identify potentially invalid records
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1 Analyze using dataframe summary
# MAGIC
# MAGIC Available statistics are:
# MAGIC ```
# MAGIC   count - mean - stddev - min - max - approximate percentiles
# MAGIC ```

# COMMAND ----------

#retail_df.describe(['InvoiceNo', 'Quantity', 'UnitPrice']).display()
retail_df.summary().select('summary', 'InvoiceNo', 'Quantity', 'UnitPrice').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 Using sql functions

# COMMAND ----------

from pyspark.sql.functions import min, max, percentile

retail_df.select(min("unitprice"), expr("max(unitprice)"), percentile("unitprice", 0.99)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>