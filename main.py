from mylib.lib import (
    start_spark,
    load_data,
    describe,
    example_transform,
    query,
    end_spark,
)

if __name__ == "__main__":
    spark = start_spark("BestRestaurantsMain")

    df = load_data(spark, "data/WorldsBestRestaurants.csv")

    describe(df)

    example_transform(df)

    sample_query = """
    SELECT `Restaurant`, Country, Rank
    FROM BestRestaurants
    WHERE  Rank < 20 
    ORDER BY Rank ASC
    """
    query(spark, df, sample_query, "BestRestaurants")

    print(end_spark(spark))
