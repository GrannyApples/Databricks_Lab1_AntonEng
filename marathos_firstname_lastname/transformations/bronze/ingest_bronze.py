from pyspark.sql import SparkSession
from utils.schemas import BRONZE_SCHEMA
from utils.helpers import write_delta


def ingest_bronze(spark: SparkSession) -> None:
    df = spark.read.csv(
        "/Volumes/marathos/default/raw/TWO_CENTURIES_OF_UM_RACES",
        header=True,
        schema=BRONZE_SCHEMA
    )

    print(f"Rows ingested: {df.count()}")
# not sure if we are meant to use deltas, but got it as a suggestion to follow most standard practice
    write_delta(df, "marathos.bronze.raw_results")
    print("Bronze table written successfully")