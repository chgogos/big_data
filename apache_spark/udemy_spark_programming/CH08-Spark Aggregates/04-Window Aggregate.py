# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #####Window Aggregation
# MAGIC Window aggregation performs calculations (like SUM, AVG, MIN, MAX) on a set of related rows (a "window").
# MAGIC
# MAGIC #####Use Cases
# MAGIC 1. Running/Moving Aggregates
# MAGIC 2. Grouped top N
# MAGIC 3. Forward/Backward comparison
# MAGIC
# MAGIC #####Structure
# MAGIC
# MAGIC ```
# MAGIC   
# MAGIC   agg_function().OVER(Window.PARTITION_BY(column_list)
# MAGIC                             .ORDER_BY(column_list)
# MAGIC                             .ROWS_BETWEEN(window_start, window_end)
# MAGIC                             
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Prepare a daily revenue report for club facility bookings as shown below.\
# MAGIC Add a running total to your report.
# MAGIC ```
# MAGIC   booked_by | booking_date    | revenue | running_total
# MAGIC   -------------------------------------------------------
# MAGIC   Guest     | 2022-07-03      | 35      | 35
# MAGIC   Guest     | 2022-07-04      | 390     | 425
# MAGIC   Guest     | 2022-07-05      | 110     | 535
# MAGIC   Guest     | 2022-07-06      | 150     | 685
# MAGIC   Member    | 2022-07-03      | 70      | 70
# MAGIC   Member    | 2022-07-04      | 107     | 177
# MAGIC   Member    | 2022-07-05      | 77      | 254
# MAGIC   Member    | 2022-07-06      | 92      | 346
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 1.1 Prepare a daily revenue report.

# COMMAND ----------

from pyspark.sql.functions import expr, sum

bookings_df = spark.table("dev.spark_db.bookings")
facilities_df = spark.table("dev.spark_db.facilities")
members_df = spark.table("dev.spark_db.members")

booking_summary_df = (
    bookings_df.join(facilities_df, "facility_id")
            .join(members_df, "member_id", "left")
            .where("month(start_time) = 7 AND year(start_time) = 2022")
            .withColumns({
                "booked_by": expr("case when member_id==0 then 'Guest' else 'Member' end"),
                "booking_date": expr("to_date(start_time)"),
                "booking_amount": expr("case when member_id == 0 then slots * guest_cost else slots * member_cost end")
                })
            .groupBy("booked_by", "booking_date")
            .agg(sum("booking_amount").alias("revenue"))
            .orderBy("booking_date")
)

booking_summary_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.2 Add running total to your report.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = (
    Window.partitionBy("booked_by")
        .orderBy("booking_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = (
    booking_summary_df.withColumn("running_total", sum("revenue").over(window_spec))
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Add a 3 day moving average to your revenue report
# MAGIC ```
# MAGIC   booked_by   | booking_date    |revenue  | 3_day_avg
# MAGIC   -------------------------------------------------------
# MAGIC   Guest       | 2022-07-03      | 35      | 35
# MAGIC   Guest       | 2022-07-04      | 390     | 212.5
# MAGIC   Gues        | 2022-07-05      | 110     | 178.33
# MAGIC   Guest       | 2022-07-06      | 150     | 216.67
# MAGIC   Guest       | 2022-07-07      | 305     | 188.33
# MAGIC   Guest       | 2022-07-08      | 550     | 335
# MAGIC   Member      | 2022-07-03      | 70      | 70
# MAGIC   Member      | 2022-07-04      | 107     | 88.5
# MAGIC   Member      | 2022-07-05      | 77      | 84.67
# MAGIC   Member      | 2022-07-06      | 92      | 92
# MAGIC   Member      | 2022-07-07      | 199     | 122.67
# MAGIC ```
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, round

window_spec = (
    Window.partitionBy("booked_by")
        .orderBy("booking_date")
        .rowsBetween(-2, Window.currentRow)
)

result_df = (
    booking_summary_df.withColumn("3_day_avg",
                                  round(avg("revenue").over(window_spec),2))
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Show top 3 revenue dates from guests and members.
# MAGIC
# MAGIC Expected Results
# MAGIC ```
# MAGIC   booked_by | booking_date  |revenue
# MAGIC   ----------------------------------
# MAGIC   Guest     | 2022-07-24    |1105
# MAGIC   Guest     | 2022-07-27    |990
# MAGIC   Guest     | 2022-07-30    |986.5
# MAGIC   Member    | 2022-07-25    |626
# MAGIC   Member    | 2022-07-31    |486
# MAGIC   Member    | 2022-07-26    |455
# MAGIC ```

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

window_spec = (
    Window.partitionBy("booked_by")
        .orderBy(col("revenue").desc())
)

result_df = (
    booking_summary_df.withColumn("rank", rank().over(window_spec))
            .where("rank <= 3")
            .drop("rank")
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC