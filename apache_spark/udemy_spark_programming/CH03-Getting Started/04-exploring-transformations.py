# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Answer the following questions using sf_fire_calls table
# MAGIC Create Dataframe from the table to start answering

# COMMAND ----------

fire_df = spark.read.table("dev.spark_db.sf_fire_calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?
# MAGIC ``` sql
# MAGIC     select count(distinct CallType) as distinct_call_type_count
# MAGIC     from dev.spark_db.sf_fire_calls
# MAGIC ```
# MAGIC

# COMMAND ----------

q1_df = (
    fire_df.selectExpr("count(distinct CallType) as distinct_call_type_count")
)

q1_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?
# MAGIC ```sql
# MAGIC   select distinct CallType as distinct_call_types
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where CallType is not null
# MAGIC ```

# COMMAND ----------

q2_df = (fire_df.where("CallType is not null")
               .selectExpr("CallType as distinct_call_type")
               .distinct()
)

q2_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?
# MAGIC ``` sql
# MAGIC   select CallNumber, Delay
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where Delay > 5
# MAGIC ```

# COMMAND ----------

(fire_df.where("Delay > 5")
       .select("CallNumber", "Delay")
       .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q4. What were the most common call types?
# MAGIC ```sql
# MAGIC   select CallType, count(*) as count
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where CallType is not null
# MAGIC   group by CallType
# MAGIC   order by count desc
# MAGIC ```

# COMMAND ----------

(fire_df.select("CallType")
    .where("CallType is not null")
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q5. What zip codes accounted for most common calls?
# MAGIC ```sql
# MAGIC   select CallType, ZipCode, count(*) as count
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where CallType is not null
# MAGIC   group by CallType, Zipcode
# MAGIC   order by count desc
# MAGIC ```

# COMMAND ----------

(fire_df.select("CallType", "ZipCode")
    .where("CallType is not null")
    .groupBy("CallType", "Zipcode")
    .count()
    .orderBy("count", ascending=False)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103
# MAGIC ```sql
# MAGIC   select distinct Neighborhood, Zipcode
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where Zipcode== 94102 or Zipcode == 94103
# MAGIC ```

# COMMAND ----------

(fire_df.select("Neighborhood", "Zipcode")
    .where("Zipcode== 94102 or Zipcode == 94103")
    .distinct()
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q7. What was the sum of all calls, average, min and max of the response times for calls?
# MAGIC ```sql
# MAGIC   select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC ```

# COMMAND ----------

(fire_df.selectExpr("sum(NumAlarms)", "avg(Delay)", "min(Delay)", "max(Delay)")
        .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q8. How many distinct years of data is in the CSV file?
# MAGIC ```sql
# MAGIC   select distinct year(CallDate) as year_num
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   order by year_num
# MAGIC ```

# COMMAND ----------

(fire_df.selectExpr("year(CallDate) as year_num")
    .distinct()
    .orderBy("year_num")
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?
# MAGIC ```sql
# MAGIC   select weekofyear(CallDate) as week_year, count(*) as count
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where year(CallDate) == 2018
# MAGIC   group by week_year
# MAGIC   order by count desc
# MAGIC ```

# COMMAND ----------

(fire_df.selectExpr("weekofyear(CallDate) as week_year")
    .where("year(CallDate) == 2018")
    .groupBy('week_year')
    .count()
    .orderBy('count', ascending=False)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?
# MAGIC ```sql
# MAGIC   select Neighborhood, Delay
# MAGIC   from dev.spark_db.sf_fire_calls
# MAGIC   where year(CallDate) == 2018
# MAGIC ```

# COMMAND ----------

(fire_df.select("Neighborhood", "Delay")
    .filter("year(CallDate) == 2018")    
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC