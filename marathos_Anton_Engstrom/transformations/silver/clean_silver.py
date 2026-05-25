from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.helpers import get_table, write_delta, add_dense_rank_id


def extract_distance_unit(df: DataFrame) -> DataFrame:
    # split event distance into unit and value
    #got help from ai for this, regex is notething i have in my head.
    #normalized the unit of km and mi uknown gets filtered out later
    return df \
        .withColumn("event_distance_unit",
            F.regexp_extract("event_distance_raw", r"([a-zA-Z]+)", 1)
        ) \
        .withColumn("event_distance_value",
            F.regexp_extract("event_distance_raw", r"([\d.]+)", 1).cast("double")
        ) \
        .withColumn("event_distance_unit",
            F.when(F.lower(F.col("event_distance_unit")).isin("km", "k"), "km")
             .when(F.lower(F.col("event_distance_unit")).isin("mi", "miles", "mile", "miles", "m"), "mi")
             .when(F.lower(F.col("event_distance_unit")) == "h", "h")
             .when(F.lower(F.col("event_distance_unit")) == "d", "d")
             .otherwise("unknown")
        )

def parse_performance(df: DataFrame) -> DataFrame:
    # using get() to safely handle malformed rows that dont have the correct format
    # regex from ai
    return df \
        .withColumn("performance_clean",
            F.trim(
                F.regexp_replace("athlete_performance_raw", r"\s*(h|km|mi|miles)$", "")
            )
        ) \
        .withColumn("performance_seconds",
            F.when(
                F.col("event_distance_unit").isin("km", "mi"),
                F.expr("""
                    try_cast(get(split(performance_clean, ':'), 0) AS INT) * 3600 +
                    try_cast(get(split(performance_clean, ':'), 1) AS INT) * 60  +
                    try_cast(get(split(performance_clean, ':'), 2) AS INT)
                """)
            ).otherwise(None)
        ) \
        .withColumn("performance_km",
            F.when(
                F.col("event_distance_unit") == "h",
                F.expr("try_cast(performance_clean AS DOUBLE)")
            ).otherwise(None)
        ) \
        .drop("performance_clean")



def cast_columns(df: DataFrame) -> DataFrame:
    # cast columns that came in as strings to their correct types if needed, migth need to include more
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
    #docs
    #unit is d treat these as invalid
    #performance could not be parsed to seconds
    #country is null
    #uknown gets filtered out, its an instance of a unit that is not km or mi(its X)
    return df.filter(
        (F.col("event_distance_unit") != "d") &
        (F.col("event_distance_unit") != "unknown") &
        (F.col("athlete_country").isNotNull()) &
        (
            (F.col("event_distance_unit").isin("km", "mi") & F.col("performance_seconds").isNotNull()) |
            (F.col("event_distance_unit") == "h")
        )
    ) 


def build_silver(spark: SparkSession) -> None:
    df = get_table("marathos.bronze.raw_results")
    print(f"Rows in bronze: {df.count()}")

    #order matters 
    df = extract_distance_unit(df)
    df = parse_performance(df)
    df = remove_invalid_rows(df)
    df = cast_columns(df)

    #there doesnt seem to be an actual unique Id for events or athletes, so we need to create them
    df = df.withColumn(
        "event_composite_key",
        F.concat_ws("_", F.col("event_name"), F.col("event_dates"), F.col("event_distance_raw"))
    )
    df = add_dense_rank_id(df, "event_composite_key", "event_id")
    df = df.drop("event_composite_key")
    df = df.withColumn(
    "athlete_composite_key",
    F.concat_ws("_",
        F.col("athlete_id_raw"),
        F.col("athlete_country"),
        F.col("athlete_year_of_birth"),
        F.col("athlete_gender"),
        F.col("athlete_age_category"),
        F.col("athlete_club")
        )
    )
    df = add_dense_rank_id(df, "athlete_composite_key", "athlete_id")
    df = df.drop("athlete_composite_key")

    print(f"Rows after cleaning: {df.count()}")

    write_delta(df, "marathos.silver.obt_results")
    print("Silver table written successfully")