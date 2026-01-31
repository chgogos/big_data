# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data from the sales_sample.csv file and analyse to identify problems

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Define schema

# COMMAND ----------

file_schema = """
id int,
name string,
dop string,
phone long,
amount string,
discount string
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Read data

# COMMAND ----------

sales_raw_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(file_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/sales_sample.csv")
)

sales_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.3 Describe the data

# COMMAND ----------

sales_raw_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.4 List down the problems you want to fix
# MAGIC 1. Convert id from integer to string and rename it as transaction_id.
# MAGIC 2. Rename the name column to customer_name.
# MAGIC 3. Convert the dop to date format and rename the column to date_of_purchase.
# MAGIC 4. Rename the phone column to customer_phone
# MAGIC 5. Convert the amount to a long value and filter out nulls and outlier values. 
# MAGIC 6. Rename the column to purchase_amount
# MAGIC 7. Convert discount to double, converting nil and null values to zero. rename the column to applied_discount

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Prepare and clean the Dataframe using appropriate transformations

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Transform

# COMMAND ----------

sales_df = sales_raw_df.selectExpr(
    "cast(id as string) as transaction_id",
    "name as customer_name",
    "nvl(try_cast(dop as date), to_date(dop, 'dd-MM-yyyy')) as date_of_purchase",
    "cast(phone as string) as customer_phone",
    "cast(amount as long) as purchase_amount",
    "nvl(try_cast(discount as double), 0) as applied_discount"
).filter("purchase_amount is not null and purchase_amount < 200000")

sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Verify statistics

# COMMAND ----------

sales_df.describe("purchase_amount", "applied_discount").display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC
# MAGIC