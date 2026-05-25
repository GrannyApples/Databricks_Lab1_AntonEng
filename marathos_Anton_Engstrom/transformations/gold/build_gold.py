from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.helpers import get_table, write_delta

#per unique event using denserank id
def build_dim_event(df: DataFrame) -> DataFrame:
    return df.select(
        "event_id",
        "event_name",
        "event_dates",
        "event_distance_raw",
        "event_distance_unit",
        "event_distance_value"
    ).distinct()

#per unique athlete using denserank id
# athlete_id_raw is not a globally unique identifier
    # it is capped at 999999 and cycles across the dataset
    # athlete_id is derived from a composite key of athlete attributes
    # two athletes with identical attributes will share an id
def build_dim_athlete(df: DataFrame) -> DataFrame:
    
    return df.select(
        "athlete_id",
        "athlete_country",
        "athlete_gender",
        "athlete_age_category",
        "athlete_year_of_birth",
        "athlete_club"
    ).distinct()

#references dim_event and dim_athlete via ids
def build_fct_results(df: DataFrame) -> DataFrame:
    
    return df.select(
        "event_id",
        "athlete_id",
        "performance_seconds",  #for km, mi events
        "performance_km",       #for h events
        "athlete_avg_speed",
        "year_of_event",
        "event_finishers"
    ).withColumn("result_id", F.monotonically_increasing_id())

#two views as required by the doc - one per marathon type
#distance events
#vw_ = view prefix
def build_views(spark: SparkSession) -> None:
   
    spark.sql("""
        CREATE OR REPLACE VIEW marathos.gold.vw_distance_results AS
        SELECT
            f.result_id,
            f.performance_seconds,
            f.athlete_avg_speed,
            f.year_of_event,
            e.event_name,
            e.event_distance_unit,
            e.event_distance_value,
            f.event_finishers,
            a.athlete_country,
            a.athlete_gender,
            a.athlete_age_category,
            a.athlete_year_of_birth
        FROM marathos.gold.fct_results f
        JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
        JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
        WHERE e.event_distance_unit IN ('km', 'mi')
    """)

#time based events
    spark.sql("""
        CREATE OR REPLACE VIEW marathos.gold.vw_timed_results AS
        SELECT
            f.result_id,
            f.performance_km,
            f.athlete_avg_speed,
            f.year_of_event,
            e.event_name,
            e.event_distance_unit,
            e.event_distance_value,
            f.event_finishers,
            a.athlete_country,
            a.athlete_gender,
            a.athlete_age_category,
            a.athlete_year_of_birth
        FROM marathos.gold.fct_results f
        JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
        JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
        WHERE e.event_distance_unit = 'h'
    """)


def build_gold(spark: SparkSession) -> None:
    df = get_table("marathos.silver.obt_results")
    print(f"rows in silver: {df.count()}")

    dim_event = build_dim_event(df)
    dim_athlete = build_dim_athlete(df)
    fct_results = build_fct_results(df)

    write_delta(dim_event,   "marathos.gold.dim_event")
    print(f"rows in dim_event {dim_event.count()}")

    write_delta(dim_athlete, "marathos.gold.dim_athlete")
    print(f"rows in dim_athlete {dim_athlete.count()}")

    write_delta(fct_results, "marathos.gold.fct_results")
    print(f"rows in fct_results {fct_results.count()}")
    print(f"rows in gold layer {fct_results.count() + dim_event.count() + dim_athlete.count()}")
    build_views(spark)
    print("Gold layer complete")