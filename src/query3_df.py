import time

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    regexp_replace,
    round as spark_round,
    when
)

from common import create_spark, CENSUS_BLOCKS_PATH, INCOME_PATH


def main():
    start_time = time.time()

    spark = create_spark("Query3_DataFrame")

    census_raw = spark.read.json(CENSUS_BLOCKS_PATH)

    population_by_zip = (
        census_raw
        .select(
            col("properties.ZCTA20").alias("zip_code"),
            col("properties.POP20").alias("population")
        )
        .filter(col("zip_code").isNotNull())
        .groupBy("zip_code")
        .agg(spark_sum("population").alias("total_population"))         #zip_code με total population
    )

    income_raw = spark.read.csv(
        INCOME_PATH,
        header=True,
        sep=";",
        inferSchema=False
    )

    income_by_zip = (
    income_raw
    .select(
        col("Zip Code").alias("zip_code"),
        col("Estimated Median Income").alias("income_raw")
    )
    .filter(col("zip_code").isNotNull())
    .withColumn(
        "median_household_income",
        when(
            col("income_raw").rlike(r"^\$[0-9,]+$"),
            regexp_replace(
                regexp_replace(col("income_raw"), "\\$", ""),
                ",",
                ""
            ).cast("double")
        )
    )
    .filter(col("median_household_income").isNotNull())
    .select("zip_code", "median_household_income")
)

    result = (
        population_by_zip
        .join(income_by_zip, on="zip_code", how="inner")
        .filter(col("total_population") > 0)
        .withColumn(
            "per_capita_income",
            spark_round(
                col("median_household_income") / col("total_population"),
                4
            )
        )
        .select(
            "zip_code",
            "total_population",
            "median_household_income",
            "per_capita_income"
        )
        .orderBy(col("zip_code").asc())
    )

    result.show(100, truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()