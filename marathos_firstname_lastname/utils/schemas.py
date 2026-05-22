from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

#  defines schema for the raw data
BRONZE_SCHEMA = StructType([
    StructField("Year of event",              StringType(),  True),
    StructField("Event dates",                StringType(),  True),
    StructField("Event name",                 StringType(),  True),
    StructField("Event distance/length",      StringType(),  True),
    StructField("Event number of finishers",  IntegerType(), True),
    StructField("Athlete performance",        StringType(),  True),
    StructField("Athlete club",               StringType(),  True),
    StructField("Athlete country",            StringType(),  True),
    StructField("Athlete year of birth",      DoubleType(),  True),
    StructField("Athlete gender",             StringType(),  True),
    StructField("Athlete age category",       StringType(),  True),
    StructField("Athlete average speed",      DoubleType(),  True),
    StructField("Athlete ID",                 IntegerType(), True),
])