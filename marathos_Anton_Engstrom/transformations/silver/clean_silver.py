from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.helpers import get_table, write_delta, add_dense_rank_id


def extract_distance_unit(df: DataFrame) -> DataFrame:
    # split event distance into unit and value
    #got help from ai for this, regex is notething i have in my head.
    return df \
        .withColumn("event_distance_unit",
            F.regexp_extract("event_distance_raw", r"([a-zA-Z]+)", 1)
        ) \
        .withColumn("event_distance_value",
            F.regexp_extract("event_distance_raw", r"([\d.]+)", 1).cast("double")
        )

def parse_performance(df: DataFrame) -> DataFrame:
    # using get() to safely handle malformed rows that dont have the correct format
    # regex from ai
    return df \
        .withColumn("performance_clean",
            F.trim(F.regexp_replace("athlete_performance_raw", r"\s*h$", ""))
        ) \
        .withColumn("performance_seconds",
            F.expr("""
                try_cast(get(split(performance_clean, ':'), 0) AS INT) * 3600 +
                try_cast(get(split(performance_clean, ':'), 1) AS INT) * 60  +
                try_cast(get(split(performance_clean, ':'), 2) AS INT)
            """)
        ) \
        .drop("performance_clean")



def cast_columns(df: DataFrame) -> DataFrame:
    # cast columns that came in as strings to their correct types if needed
    return df \
        .withColumn("athlete_year_of_birth",
            F.expr("try_cast(try_cast(athlete_year_of_birth AS DOUBLE) AS INT)")
        ) \
        .withColumn("athlete_avg_speed",
            F.expr("try_cast(athlete_avg_speed AS DOUBLE)")
        ) \
        .withColumn("year_of_event",
            F.expr("try_cast(year_of_event AS INT)")
        ) \
        .withColumn("event_finishers",
            F.expr("try_cast(event_finishers AS INT)")
        )


def remove_invalid_rows(df: DataFrame) -> DataFrame:
    # docs
    #unit is d treat these as invalid
    #performance could not be parsed to seconds
    #country is null
    return df.filter(
        (F.col("event_distance_unit") != "d") &
        (F.col("event_distance_unit") != "") &
        (F.col("performance_seconds").isNotNull()) &
        (F.col("athlete_country").isNotNull())
    )


def build_silver(spark: SparkSession) -> None:
    df = get_table("marathos.bronze.raw_results")
    print(f"Rows in bronze: {df.count()}")

    #found out order matters 
    df = extract_distance_unit(df)
    df = parse_performance(df)
    df = remove_invalid_rows(df)
    df = cast_columns(df)
    df = add_dense_rank_id(df, "event_name", "event_id")
    df = add_dense_rank_id(df, "athlete_id_raw", "athlete_id")

    print(f"Rows after cleaning: {df.count()}")

    write_delta(df, "marathos.silver.obt_results")
    print("Silver table written successfully")