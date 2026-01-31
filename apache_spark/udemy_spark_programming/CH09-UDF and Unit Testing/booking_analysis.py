#Pyspark Module for booking analysis

def read_booking_summary(spark):
    file_schema = "booked_by string, booking_date date, revenue double"

    summary_df = (
        spark.read.format("csv")
            .option("header", "true")
            .schema(file_schema)
            .load("/Volumes/dev/spark_db/datasets/spark_programming/data/booking-summary.csv")
    )
    return summary_df

def top_3_revenue(summary_df):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, rank

    window_spec = (
        Window.partitionBy("booked_by")
            .orderBy(col("revenue").desc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result_df = (
        summary_df
            .withColumn("rank", rank().over(window_spec))
            .where("rank <= 3")
            .drop("rank")
    )
    return result_df