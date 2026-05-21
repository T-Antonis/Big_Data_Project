import time

from common import create_spark, read_crime_data, CRIME_PARQUET_PATH


def main():
    start_time = time.time()

    spark = create_spark("Convert_Crime_CSV_To_Parquet")

    crimes = read_crime_data(spark)

    crimes.write.mode("overwrite").parquet(CRIME_PARQUET_PATH)

    end_time = time.time()
    print(f"CSV to Parquet conversion time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()