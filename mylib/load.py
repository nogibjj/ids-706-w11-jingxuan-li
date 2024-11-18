from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession

def load_data(spark, file_path, name="BestRestaurants"):
    """
    from dbfs load to Spark DataFrame。
    """
    print(f"start load data:{file_path}")

    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Rank", IntegerType(), True),
        StructField("Restaurant", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lng", FloatType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(file_path)
    # Register as global temp view
    df.createOrReplaceGlobalTempView(name)
    print(f"Global temporary table '{name}' registered successfully")
    print("load data successfully")
    display(df.limit(10))
    return df

def describe(df):
    display(df.describe())
    return df.describe()

def example_transform(df):
    print("start transform data")
    conditions = [
        (col("Country") == "Spain"),
        (col("Rank") < 20),
    ]
    categories = ["Spanish Restaurant", "High Scoring Restaurant"]
    df = df.withColumn("Category", when(
        conditions[0], categories[0]
        ).when(conditions[1], categories[1]).otherwise("Other"))
    print("transform data finished")
    display(df.limit(10))
    return df

def main():
    file_path = "dbfs:/FileStore/ids-706-w11-jingxuan-li/WorldsBestRestaurants.csv"
    spark = SparkSession.builder.appName("BestRestaurantsMain").getOrCreate()

    # load data
    df = load_data(spark, file_path)

    describe(df)

    # execute load
    example_transform(df)

if __name__ == "__main__":
    main()
