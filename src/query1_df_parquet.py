import time

from pyspark.sql.functions import col, count, round as spark_round, sum as spark_sum, when

from common import create_spark, CRIME_PARQUET_PATH


def main():
    start_time = time.time()

    spark = create_spark("Query1_DataFrame_Parquet")

    df = spark.read.parquet(CRIME_PARQUET_PATH)

    result = (
        df
        .filter(col("Premis Desc") == "STREET")
        .withColumn(
            "hour",
            (col("TIME OCC").cast("int") / 100).cast("int")
        )
        .withColumn(
            "day_part",
            when((col("hour") >= 5) & (col("hour") <= 11), "Morning")
            .when((col("hour") >= 12) & (col("hour") <= 16), "Afternoon")
            .when((col("hour") >= 17) & (col("hour") <= 20), "Evening")
            .otherwise("Night")
        )
        .groupBy("day_part")
        .agg(count("*").alias("crime_count"))
    )

    total = result.agg(spark_sum("crime_count").alias("total")).collect()[0]["total"]

    final_result = (
        result
        .withColumn(
            "percentage",
            spark_round((col("crime_count") / total) * 100, 2)
        )
        .orderBy(col("percentage").desc())
    )

    final_result.show(truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()