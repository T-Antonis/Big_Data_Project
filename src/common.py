from pyspark.sql import SparkSession


HDFS_BASE = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"

CRIME_DATA_PATH = f"{HDFS_BASE}/data/LA_Crime_Data/*.csv"
CRIME_2010_2019_PATH = f"{HDFS_BASE}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020_2025_PATH = f"{HDFS_BASE}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

CENSUS_BLOCKS_PATH = f"{HDFS_BASE}/data/LA_Census_Blocks_2020.geojson"
CENSUS_FIELDS_PATH = f"{HDFS_BASE}/data/LA_Census_Blocks_2020_fields.csv"
INCOME_PATH = f"{HDFS_BASE}/data/LA_income_2021.csv"
POLICE_STATIONS_PATH = f"{HDFS_BASE}/data/LA_Police_Stations.csv"
CRIME_PARQUET_PATH = f"{HDFS_BASE}/user/dsml00284/dsml-bigdata-project/LA_Crime_Data_Parquet"


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_crime_data(spark: SparkSession):
    return spark.read.csv(
        CRIME_DATA_PATH,
        header=True,
        inferSchema=False
    )