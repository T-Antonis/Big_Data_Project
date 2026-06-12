import time
from csv import reader

from common import create_spark, CRIME_2010_2019_PATH, CRIME_2020_2025_PATH


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

    spark = create_spark("Query1_RDD")
    sc = spark.sparkContext

    paths = f"{CRIME_2010_2019_PATH},{CRIME_2020_2025_PATH}"

    raw = sc.textFile(paths)

    header = raw.first()         #παίρνουμε τη πρώτη γραμμή του αρχείου ώστε να αφαιρεθεί στη συνέχεια

    rows = (
        raw
        .filter(lambda line: line != header)
        .mapPartitions(lambda lines: reader(lines))           #μετατροπή σε λίστες
    )

  
    street_rows = rows.filter(
        lambda row: len(row) > 15 and row[15] == "STREET"
    )

    counts = (
        street_rows
        .map(lambda row: (get_day_part(row[3]), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    total = counts.map(lambda x: x[1]).sum()

    result = (
        counts
        .map(lambda x: (x[0], x[1], round((x[1] / total) * 100, 2)))
        .sortBy(lambda x: x[2], ascending=False)
        .collect()
    )

    print("+---------+-----------+----------+")
    print("|day_part |crime_count|percentage|")
    print("+---------+-----------+----------+")
    for day_part, crime_count, percentage in result:
        print(f"|{day_part:<9}|{crime_count:<11}|{percentage:<10}|")
    print("+---------+-----------+----------+")

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()