import json
import time
from csv import reader

from common import create_spark, CENSUS_BLOCKS_PATH, INCOME_PATH


def clean_income_value(income_text):
    if income_text is None:
        return None

    income_text = income_text.strip()

    
    if not income_text.startswith("$"):
        return None

    try:
        return float(income_text.replace("$", "").replace(",", ""))
    except ValueError:
        return None


def parse_census_line(line):
    try:
        line = line.strip()

        # Κρατάμε μόνο γραμμές που μοιάζουν με Feature object
        if not line.startswith('{ "type": "Feature"') and not line.startswith('{"type":"Feature"'):
            return None

        
        if line.endswith(","):
            line = line[:-1]

        record = json.loads(line)
        properties = record.get("properties", {})

        zip_code = properties.get("ZCTA20")
        population = properties.get("POP20")

        if zip_code is None or population is None:
            return None

        return zip_code, int(population)

    except Exception:
        return None


def main():
    start_time = time.time()

    spark = create_spark("Query3_RDD")
    sc = spark.sparkContext

    # Census GeoJSON:
    # Διαβάζεται ως text. Κάθε κανονική γραμμή feature περιέχει JSON object
    # με properties.ZCTA20 και properties.POP20.
    
    census_raw = sc.textFile(CENSUS_BLOCKS_PATH)  

    population_by_zip = (
        census_raw
        .map(parse_census_line)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
    )

    income_raw = sc.textFile(INCOME_PATH)                             #Το income csv χρησιμοποιεί delimiter ";"
    income_header = income_raw.first()

    income_by_zip = (
        income_raw
        .filter(lambda line: line != income_header)
        .mapPartitions(lambda lines: reader(lines, delimiter=";"))
        .filter(lambda row: len(row) >= 3)
        .map(lambda row: (row[0], clean_income_value(row[2])))
        .filter(lambda x: x[0] is not None and x[1] is not None)
    )
    
    income_dict = income_by_zip.collectAsMap()                        #μορφή Python dictionary / επιτρεπτό το collectAsMap λόγω μικρού dataset
    income_broadcast = sc.broadcast(income_dict)

    result = (
        population_by_zip
        .filter(lambda x: x[0] in income_broadcast.value)
        .filter(lambda x: x[1] > 0)
        .map(
            lambda x: (
                x[0],
                x[1],
                income_broadcast.value[x[0]],
                round(income_broadcast.value[x[0]] / x[1], 4)
            )
        )
        .sortBy(lambda x: x[0])
        .collect()
    )
    
    
    result = sorted(result, key=lambda x: x[0])

    print("+--------+----------------+-----------------------+-----------------+")
    print("|zip_code|total_population|median_household_income|per_capita_income|")
    print("+--------+----------------+-----------------------+-----------------+")

    for zip_code, population, income, per_capita in result:
        print(f"|{zip_code:<8}|{population:<16}|{income:<23}|{per_capita:<17}|")

    print("+--------+----------------+-----------------------+-----------------+")

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()



if __name__ == "__main__":
    main()