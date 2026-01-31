# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Setup Catalog, Database and Volume
# MAGIC 1. Create a catalog - dev
# MAGIC 2. Create a database - dev.spark_db
# MAGIC 3. Create a volume - dev.spark_db.datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev;
# MAGIC CREATE DATABASE IF NOT EXISTS dev.spark_db;
# MAGIC CREATE VOLUME IF NOT EXISTS dev.spark_db.datasets;

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Import data ingestion utility

# COMMAND ----------

# MAGIC %run ../utils/data-ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Ingest Course Datasets

# COMMAND ----------

DL = DataLoader()
DL.clean_ingest_data("spark_programming/data", "spark_programming/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Create a table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.spark_db.diamonds(
# MAGIC     carat DOUBLE,
# MAGIC     clarity STRING,
# MAGIC     color STRING,
# MAGIC     cut STRING,
# MAGIC     depth STRING,
# MAGIC     price DOUBLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Load course data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev.spark_db.diamonds 
# MAGIC SELECT * FROM json.`/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT color, avg(price) AS avg_price
# MAGIC FROM dev.spark_db.diamonds
# MAGIC GROUP BY color
# MAGIC ORDER BY avg_price DESC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC