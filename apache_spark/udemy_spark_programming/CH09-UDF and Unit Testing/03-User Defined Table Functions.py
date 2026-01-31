# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Python UDTF
# MAGIC 1. User-defined function that returns a table 
# MAGIC 2. Take Python Objects as input
# MAGIC 3. Operate one row at a time
# MAGIC 4. Serialized/Deserialized by pickle or Arrow 

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How to define and use a Python UDTF

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Define a UDTF\
# MAGIC Create a UDTF as the following.
# MAGIC * Input: start_date (ex: 2025-07-27), expand_days (ex: 5)
# MAGIC * Output: expand the given start_date to expand_days as below.
# MAGIC ```
# MAGIC     +----------+
# MAGIC     |date_value|
# MAGIC     +----------+
# MAGIC     |2025-07-27|
# MAGIC     |2025-07-28|
# MAGIC     |2025-07-29|
# MAGIC     |2025-07-30|
# MAGIC     |2025-07-31|
# MAGIC     +----------+
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import udtf
from datetime import datetime, timedelta

@udtf(returnType="date_value: string", useArrow=True)
class DateExploder:
    def eval(self, start_date: str, expand_days: int):
        current = datetime.strptime(start_date, "%Y-%m-%d")
        for i in range(expand_days):
            yield (current.strftime("%Y-%m-%d"), )
            current += timedelta(days=1)


# COMMAND ----------

from pyspark.sql.functions import lit

DateExploder(lit("2025-07-27"), lit(6)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Use a UDTF in Dataframe Transformations

# COMMAND ----------

data_schema = "id int, name string, join_date string"
data_list = [(101, "Prashant", "2025-02-25"),
             (102, "Sushant", "2025-02-26")]

students_df = spark.createDataFrame(data_list, data_schema).alias("s")

result_df = (
    students_df.lateralJoin(DateExploder("s.join_date", lit(5)))
        .selectExpr("id", "name", "join_date", "date_value as attendance_date")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.3. Register UDTF for use in Spark SQL
# MAGIC

# COMMAND ----------

spark.udtf.register("date_exploder", DateExploder)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.4 UDTF from Spark SQL
# MAGIC

# COMMAND ----------

students_df.createOrReplaceTempView("students_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from students_view, lateral date_exploder(join_date, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC