# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pandas UDF (Vectorized UDF)
# MAGIC 1. User-defined functions 
# MAGIC 2. Take or return Pandas Series/DataFrame
# MAGIC 3. Operate block by block (Vectorized)
# MAGIC 4. Serialized/Deserialized by Arrow
# MAGIC
# MAGIC Note: You must know Pandas to work with the data inside the function

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How to define and use a Pandas UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Create a Pandas UDF with the following functionality
# MAGIC * Input -> member_id
# MAGIC * Output -> Guest/Member

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def member_type_pudf(id: pd.Series)-> pd.Series:
    return id.apply(lambda x: 'Guest' if x==0 else 'Member')


# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Use a Pandas UDF in Dataframe Transformations\
# MAGIC List all bookings made by guests

# COMMAND ----------

bookings_df = spark.table("dev.spark_db.bookings")

bookings_df.where(member_type_pudf("member_id")=="Guest").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.3. Register Pandas UDF for use in Spark SQL
# MAGIC

# COMMAND ----------

spark.udf.register("get_member_type", member_type_pudf)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.4 Pandas UDF from Spark SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.spark_db.bookings
# MAGIC where get_member_type(member_id)=="Guest"

# COMMAND ----------

# MAGIC %md
# MAGIC 1.5 Pandas UDF in Expressions

# COMMAND ----------

result_df = bookings_df.where("get_member_type(member_id)=='Guest'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC