from pyspark.sql import SparkSession

def query(spark, query_str):
    """
    Execute a query on the global temporary table.
    """
    print(f"Executing query: {query_str}")
    result_df = spark.sql(query_str)
    result_df.show(10, truncate=False)
    print("Query execution completed")
    return result_df

def main():
    """
    Main function: execute query on the registered global temp table.
    """
    spark = SparkSession.builder.appName("QueryExecution").getOrCreate()

    # Define SQL query
    query_str = """
    SELECT Restaurant, Country, Rank
    FROM global_temp.BestRestaurants
    WHERE Rank < 20
    ORDER BY Rank ASC
    """

    # Execute the query
    query(spark, query_str)



if __name__ == "__main__":
    main()


