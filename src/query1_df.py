import time

from pyspark.sql.functions import col, when, floor, count, sum as spark_sum, round as spark_round

from common import create_spark, read_crime_data


def main():
    start_time = time.time()
    spark = create_spark("Query1_DataFrame_No_UDF")

    crimes = read_crime_data(spark)

    street_crimes = (
        crimes
        .filter(col("Premis Desc") == "STREET")
        .withColumn("hour", floor(col("TIME OCC").cast("int") / 100))        #μετατροπή ώρας από ΗΗΜΜ σε ώρα ημέρας για τα τμήματα μέσα στη μέρα
        .withColumn(
            "day_part",
            when((col("hour") >= 5) & (col("hour") <= 11), "Morning")
            .when((col("hour") >= 12) & (col("hour") <= 16), "Afternoon")
            .when((col("hour") >= 17) & (col("hour") <= 20), "Evening")
            .otherwise("Night")
        )
    )

    counts = (
        street_crimes
        .groupBy("day_part")
        .agg(count("*").alias("crime_count"))
    )

    total = counts.agg(spark_sum("crime_count").alias("total_count"))

    result = (
        counts
        .crossJoin(total)
        .withColumn(
            "percentage",
            spark_round((col("crime_count") / col("total_count")) * 100, 2)
        )
        .select("day_part", "crime_count", "percentage")
        .orderBy(col("percentage").desc())
    )

    result.show(truncate=False)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds ")

    spark.stop()


if __name__ == "__main__":
    main()