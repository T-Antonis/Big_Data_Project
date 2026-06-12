import time

from common import create_spark, read_crime_data


def main():
    start_time = time.time()

    spark = create_spark("Query2_SQL")

    crimes = read_crime_data(spark)

    crimes.createOrReplaceTempView("crimes")                         

    result = spark.sql("""
        WITH crimes_with_date AS (
            SELECT
                YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS year,
                MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month
            FROM crimes
            WHERE `DATE OCC` IS NOT NULL
        ),

        monthly_counts AS (
            SELECT
                year,
                month,
                COUNT(*) AS crime_total
            FROM crimes_with_date
            WHERE year IS NOT NULL
              AND month IS NOT NULL
            GROUP BY year, month
        ),

        ranked_months AS (
            SELECT
                year,
                month,
                crime_total,
                ROW_NUMBER() OVER (
                    PARTITION BY year
                    ORDER BY crime_total DESC, month ASC
                ) AS ranking
            FROM monthly_counts
        )

        SELECT
            year,
            month,
            crime_total,
            ranking
        FROM ranked_months
        WHERE ranking <= 3
        ORDER BY year ASC, ranking ASC
    """)

    result.show(100, truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()