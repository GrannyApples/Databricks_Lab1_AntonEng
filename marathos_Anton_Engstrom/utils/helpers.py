from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# Asked AI for commonly used helpers for databricks and pyspark

def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_table(table_name: str) -> DataFrame:
    spark = get_spark()
    return spark.table(table_name)


def write_delta(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

def add_dense_rank_id(df: DataFrame, order_col: str, id_col: str) -> DataFrame:
    w = Window.orderBy(order_col)
    return df.withColumn(id_col, F.dense_rank().over(w))
