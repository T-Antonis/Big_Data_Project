import time

from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    round as spark_round,
    sqrt,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from common import create_spark, read_crime_data, POLICE_STATIONS_PATH


def main():
    start_time = time.time()

    spark = create_spark("Query4_DataFrame")

    crimes_raw = read_crime_data(spark)

    crimes = (
        crimes_raw
        .select(
            col("DR_NO").alias("crime_id"),
            col("LAT").cast("double").alias("crime_lat"),
            col("LON").cast("double").alias("crime_lon")
        )
        .filter(col("crime_id").isNotNull())
        .filter(col("crime_lat").isNotNull() & col("crime_lon").isNotNull())
        .filter((col("crime_lat") != 0) & (col("crime_lon") != 0))                #πετάμε γραμμές με συντεταγμένες (0,0) - άκυρη/κενή τοποθεσία
    )
    

    stations_raw = spark.read.csv(
        POLICE_STATIONS_PATH,
        header=True,
        inferSchema=False
    )

    stations = (
        stations_raw
        .select(
            col("DIVISION").alias("division"),
            col("Y").cast("double").alias("station_lat"),
            col("X").cast("double").alias("station_lon")
        )
        .filter(col("division").isNotNull())
        .filter(col("station_lat").isNotNull() & col("station_lon").isNotNull())
    )

    pairs = crimes.crossJoin(stations)

    distance_expr = sqrt(
    (col("crime_lat") - col("station_lat")) ** 2 +
    (col("crime_lon") - col("station_lon")) ** 2
    )

    distances = pairs.withColumn("distance", distance_expr)

    nearest_window = (
        Window
        .partitionBy("crime_id")
        .orderBy(col("distance").asc())
    )

    nearest_station_per_crime = (
        distances
        .withColumn("rn", row_number().over(nearest_window))
        .filter(col("rn") == 1)
    )

    result = (
        nearest_station_per_crime
        .groupBy("division")
        .agg(
            spark_round(avg("distance"), 3).alias("average_distance"),
            count("*").alias("#")
        )
        .orderBy(col("#").desc())
    )

    print("\n=== Query 4 execution plan ===")
    result.explain(mode="formatted")

    print("\n=== Query 4 result ===")
    result.show(50, truncate=False)

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()