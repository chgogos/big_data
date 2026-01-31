# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data from the apache-logs.txt file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Load and display the data

# COMMAND ----------

file_df = (
    spark.read
        .format("text")
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/apache-logs.txt")
)

display(file_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Print the schema

# COMMAND ----------

file_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Develop an strategy to extract the following fields
# MAGIC 1. ip_address: It is the IP address of the site visitor.
# MAGIC 2. visit_timestamp: It is the date and time of the site visit. Parse and format the timestamp to YYYY-MM-DD HH:MI:SS Z
# MAGIC 3. visit_resource: Which resource from our website was accessed
# MAGIC 4. referring_url: It is the clean URL of the referring website.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Develop a regular expression

# COMMAND ----------

log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Apply regular expression to parse the record

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

logs_df = (
    file_df.select(
        regexp_extract("value", log_reg, 1).alias("ip_address"),
        regexp_extract("value", log_reg, 4).alias("visit_timestamp"),
        regexp_extract('value', log_reg, 6).alias('visit_resource'),
        regexp_extract('value', log_reg, 10).alias('referring_url')
    )
)

logs_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Refine results with further transformations

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, substring_index

test_df = (
    logs_df.withColumns({
        "visit_timestamp": to_timestamp("visit_timestamp", "dd/MMM/yyyy:HH:mm:ss Z"),
        "referring_url": substring_index("referring_url", "/", 3)
    })
)

test_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Use AI to parse and extract the required information from unstructured data

# COMMAND ----------

# MAGIC %md
# MAGIC 3.1 Prepare an AI prompt to extract the required information
# MAGIC

# COMMAND ----------

prompt = """
You will be provided with an Apache log file record. It is an unstructured text record. 
Each record represents some information for our website visits, such as what is the IP address of the visitor, 
What is the date and time of the visit, which resource was requested, and the URL of the referring website? 
You are asked to parse the log file record and extract the following fields.
ip_address: It is the IP address of the site visitor.
visit_timestamp: It is the date and time of the site visit. Parse and format the timestamp to YYYY-MM-DD HH:MI:SS Z
visit_resource: Which resource from our website was accessed?
referring_url: It is the clean URL of the referring website. When the actual referring URL is not given, 
you can extract the URL from the user agent. For cleaning the URL, you should take the values only up to the domain extension, 
such as .com, .in, .uk, etc.
Give only the final answer in the JSON format.
Record:
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 3.2 Develop an AI query expression

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, expr

result_df = (
    file_df.limit(20)
        .withColumn("prompt", concat(lit(prompt), col("value")))
        .withColumn("json_extract", expr("""
            ai_query(
                endpoint=> 'databricks-llama-4-maverick',
                request=> prompt,
                responseFormat=> 'struct<extract: struct<
                ip_address: string,
                visit_timestamp: string,
                visit_resource: string,
                referring_url: string
                >>')"""))
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4.3 Parse the JSON extract to individual columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import from_json, col, to_timestamp

extract_schema = "ip_address string, visit_timestamp string, visit_resource string, referring_url string"

final_result_df = (
    result_df.withColumn("json_extract", from_json(col("json_extract"), extract_schema))
             .selectExpr("json_extract.*")
             .withColumn("visit_timestamp", to_timestamp(col("visit_timestamp"), "yyyy-MM-dd HH:mm:ss Z"))
)

final_result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC
# MAGIC