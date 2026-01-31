# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Multilevel Aggregates
# MAGIC Multilevel aggregates allows summarizing data at several hierarchical levels.\
# MAGIC We have three variations of multilevel aggregates in Spark.
# MAGIC 1. Rollup
# MAGIC 2. Cube
# MAGIC 3. Grouping Sets
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement - Analysis Data Set
# MAGIC
# MAGIC Prepare club bookings dataset for analysis
# MAGIC ```
# MAGIC +----------+------------+---------------+-------------------+--------------+
# MAGIC |booking_id| member_name|  facility_name|         start_time|booking_amount|
# MAGIC +----------+------------+---------------+-------------------+--------------+
# MAGIC ```

# COMMAND ----------

bookings_df = spark.table("dev.spark_db.bookings")
facilities_df = spark.table("dev.spark_db.facilities")
members_df = spark.table("dev.spark_db.members")

club_bookings_df = (
    bookings_df.join(facilities_df, "facility_id")
            .join(members_df, "member_id", "left")
            .selectExpr("member_id", "booking_id",
                        "case when member_id==0 then 'Guest Member' else concat_ws(' ', first_name, last_name) end as member_name",
                        "facility_name","start_time",
                        "case when member_id == 0 then slots * guest_cost else slots * member_cost end as booking_amount")
)

club_bookings_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Prepare a monthly revenue report for year 2022.
# MAGIC
# MAGIC Also roll up the total for the month column.
# MAGIC ```
# MAGIC +----+--------+
# MAGIC |mnth| revenue|
# MAGIC +----+--------+
# MAGIC |   7| 23202.5|
# MAGIC |   8| 46066.5|
# MAGIC |   9| 63315.5|
# MAGIC |    |132584.5|
# MAGIC +----+--------+
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import month, sum, col

result_df = (
    club_bookings_df.where("year(start_time) == 2022")
        .withColumn("mnth", month("start_time"))
        .rollup("mnth")
        .agg(sum("booking_amount").alias("revenue"))
        .orderBy(col("mnth").asc_nulls_last())
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Prepare a revenue report by revenue_from (Guest/Member) and facility_name for year 2022.
# MAGIC
# MAGIC Also roll up the total for each group.
# MAGIC ```
# MAGIC +------------+---------------+-------+
# MAGIC |revenue_from|  facility_name|revenue|
# MAGIC +------------+---------------+-------+
# MAGIC |       Guest|Badminton Court| 1906.5|
# MAGIC |       Guest| Massage Room 1|41600.0|
# MAGIC |       Guest| ..............|.......|
# MAGIC |       Guest|     ..........|  .....|
# MAGIC |       Guest|           NULL|89096.5|
# MAGIC |      Member|Badminton Court|    0.0|
# MAGIC |      Member| Massage Room 1|30940.0|
# MAGIC |      Member| ..............| ......|
# MAGIC |      Member| ..............| ......|
# MAGIC |      Member|           NULL|43488.0|
# MAGIC +------------+---------------+-------+
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import sum, col, expr

result_df = (
    club_bookings_df.where("year(start_time) == 2022")
        .withColumn("revenue_from", expr("case when member_id==0 then 'Guest' else 'Member' end"))
        .rollup("revenue_from", "facility_name")
        .agg(sum("booking_amount").alias("revenue"))
        .orderBy(col("revenue_from").asc_nulls_last(),
                 col("facility_name").asc_nulls_last())
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Prepare a revenue report by revenue_from(Guest/Member) and facility_name for year 2022.
# MAGIC
# MAGIC Also compute totals for all 4 dimensions of revenue_from and facility_name.
# MAGIC * (revenue_from, facility_name) : rollup
# MAGIC * (revenue_from, ) : rollup
# MAGIC * (facility_name, ) : not available in roolup
# MAGIC * ( , ) : grand total in rollup

# COMMAND ----------

from pyspark.sql.functions import sum, col, expr

result_df = (
    club_bookings_df.where("year(start_time) == 2022")
        .withColumn("revenue_from", expr("case when member_id==0 then 'Guest' else 'Member' end"))
        .cube("revenue_from", "facility_name")
        .agg(sum("booking_amount").alias("revenue"))
        .orderBy(col("revenue_from").asc_nulls_last(),
                 col("facility_name").asc_nulls_last())
)

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q4: Prepare a revenue report similar to the following.
# MAGIC ```
# MAGIC   revenue_from  | facility_name   | revenue
# MAGIC   ---------------------------------------
# MAGIC   Guest         | Badminton Court | 1906.5
# MAGIC   Guest         | Massage Room 1  | 41600
# MAGIC   Guest         | Massage Room 2  | 13920
# MAGIC   Guest         |                 | 57426.5
# MAGIC   Member        | Badminton Court | 0
# MAGIC   Member        | Massage Room 1  | 30940
# MAGIC   Member        | Massage Room 2  | 1890
# MAGIC   Member        |                 | 32830
# MAGIC ```
# MAGIC Roll up the total for a the facility_name only.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum, col, expr

result_df = (
    club_bookings_df.where("year(start_time) == 2022")
        .withColumn("revenue_from", expr("case when member_id==0 then 'Guest' else 'Member' end"))
        .groupingSets([("revenue_from","facility_name"), ("revenue_from", )], "revenue_from", "facility_name")
        .agg(sum("booking_amount").alias("revenue"))
        .orderBy(col("revenue_from").asc_nulls_last(),
                 col("facility_name").asc_nulls_last())
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