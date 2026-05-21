import time
from pyspark.sql.functions import col, month, year, count, row_number, to_timestamp
from pyspark.sql.window import Window
from common import create_spark, read_crime_data


def main():
    start_time = time.time()

    spark = create_spark("Query2_DataFrame")

    crimes = read_crime_data(spark)

    crimes_with_date = (
        crimes
        .withColumn("date_occ", to_timestamp(col("DATE OCC"), "yyyy MMM dd hh:mm:ss a"))         #date occ είναι string άρα χρειάζεται μετατροπή
        .withColumn("year", year(col("date_occ")))
        .withColumn("month", month(col("date_occ")))
        .filter(col("year").isNotNull() & col("month").isNotNull())
    )

    monthly_counts = (
        crimes_with_date
        .groupBy("year", "month")
        .agg(count("*").alias("crime_total"))
    )

    window_spec = (
        Window                                                        #Το window επιτρέπει να κάνουμε υπολογισμούς μέσα σε μια ομάδα γραμμών
        .partitionBy("year")
        .orderBy(col("crime_total").desc(), col("month").asc())
    )

    result = (
        monthly_counts
        .withColumn("ranking", row_number().over(window_spec))
        .filter(col("ranking") <= 3)
        .select("year", "month", "crime_total", "ranking")
        .orderBy(col("year").asc(), col("ranking").asc())
    )

    result.show(100,truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()