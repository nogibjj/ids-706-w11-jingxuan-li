from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import (
    StructType,
    StructField, 
    IntegerType, 
    StringType, 
    FloatType
    )

LOG_FILE = "pyspark_output.md"

def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def load_data(spark, data="WorldsBestRestaurants.csv", name="BestRestaurants"):
    """load data with a schema matching the CSV file"""
    # Adjust schema based on your CSV structure
    # year,rank,restaurant,location,country,lat,lng
    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Rank", IntegerType(), True),
        StructField("Restaurant", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()

def query(spark, df, query_str, name): 
    """queries using spark sql"""
    df.createOrReplaceTempView(name)
    result_df = spark.sql(query_str)
    log_output("query data", result_df.limit(10).toPandas().to_markdown(), query_str)
    return result_df.show()

def example_transform(df):
    """does an example transformation on the restaurant dataset"""
    conditions = [
        (col("Country") == "Spain"),
        (col("Rank") < 20),
    ]

    categories = ["Spanish Restaurant", "High Scoring Restaurant"]

    df = df.withColumn("Category", when(
        conditions[0], categories[0]
        ).when(conditions[1], categories[1]).otherwise("Other"))

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()
