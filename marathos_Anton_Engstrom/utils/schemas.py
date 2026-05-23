from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

BRONZE_SCHEMA = StructType([
    StructField("year_of_event",             StringType(), True),
    StructField("event_dates",               StringType(), True),
    StructField("event_name",                StringType(), True),
    StructField("event_distance_raw",        StringType(), True),
    StructField("event_finishers",           StringType(), True),
    StructField("athlete_performance_raw",   StringType(), True),
    StructField("athlete_club",              StringType(), True),
    StructField("athlete_country",           StringType(), True),
    StructField("athlete_year_of_birth",     StringType(), True),
    StructField("athlete_gender",            StringType(), True),
    StructField("athlete_age_category",      StringType(), True),
    StructField("athlete_avg_speed",         StringType(), True),
    StructField("athlete_id_raw",            StringType(), True),
])
#stringtype in bronze layer, silver layers responsibility to cast match.