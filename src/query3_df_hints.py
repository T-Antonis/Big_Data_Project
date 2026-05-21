import sys
import time

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    regexp_replace,
    round as spark_round,
    when
)

from common import create_spark, CENSUS_BLOCKS_PATH, INCOME_PATH


def apply_join_hint(df, hint_name):
    hint_name = hint_name.upper()

    if hint_name == "NONE":
        return df

    if hint_name in {"BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"}:
        return df.hint(hint_name)

    raise ValueError(
        "Unknown hint. Use one of: NONE, BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL"
    )


def main():
    start_time = time.time()

    hint_name = sys.argv[1].upper() if len(sys.argv) > 1 else "NONE"

    spark = create_spark(f"Query3_DataFrame_{hint_name}")

    census_raw = spark.read.json(CENSUS_BLOCKS_PATH)

    population_by_zip = (
        census_raw
        .select(
            col("properties.ZCTA20").alias("zip_code"),
            col("properties.POP20").alias("population")
        )
        .filter(col("zip_code").isNotNull())
        .groupBy("zip_code")
        .agg(spark_sum("population").alias("total_population"))
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

    hinted_income = apply_join_hint(income_by_zip, hint_name)

    result = (
        population_by_zip
        .join(hinted_income, on="zip_code", how="inner")
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

    print(f"\n=== Query 3 with hint: {hint_name} ===")
    print("\n=== Query 3 execution plan ===")
    result.explain(mode="formatted")

    print("\n=== Query 3 result ===")
    result.show(100, truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()
