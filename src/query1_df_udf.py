import time

from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round, udf
from pyspark.sql.types import StringType

from common import create_spark, read_crime_data


def get_day_part(time_occ):
    if time_occ is None:
        return "Unknown"

    try:
        hour = int(time_occ) // 100
    except (ValueError, TypeError):
        return "Unknown"

    if 5 <= hour <= 11:
        return "Morning"
    elif 12 <= hour <= 16:
        return "Afternoon"
    elif 17 <= hour <= 20:
        return "Evening"
    else:
        return "Night"


def main():
    start_time = time.time()

    spark = create_spark("Query1_DataFrame_UDF")

    crimes = read_crime_data(spark)

    day_part_udf = udf(get_day_part, StringType())                  #μετατροπή συνάρτησης σε Spark UDF για εφαρμογή σε στήλες df

    street_crimes = (
        crimes
        .filter(col("Premis Desc") == "STREET")
        .withColumn("day_part", day_part_udf(col("TIME OCC")))
    )

    counts = (
        street_crimes
        .groupBy("day_part")
        .agg(count("*").alias("crime_count"))
    )

    total = counts.agg(
        spark_sum("crime_count").alias("total_count")
    )

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
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()