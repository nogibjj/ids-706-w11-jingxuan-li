import pytest
from mylib.lib import (
    start_spark,
    load_data,
    describe,
    example_transform,
    query,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    """Fixture to set up and tear down Spark session"""
    spark_session = start_spark("TestBestRestaurants")
    yield spark_session
    end_spark(spark_session)


def test_load_data(spark):
    """Test the load_data function"""
    df = load_data(spark, "data/WorldsBestRestaurants.csv")
    assert df is not None, "DataFrame should not be None"
    assert df.count() > 0, "DataFrame should not be empty"


def test_describe(spark):
    """Test the describe function"""
    df = load_data(spark, "data/WorldsBestRestaurants.csv")
    # Ensure describe function runs without errors
    assert describe(df) is None, "Describe function should not return anything"


def test_example_transform(spark):
    """Test the example_transform function"""
    df = load_data(spark, "data/WorldsBestRestaurants.csv")
    # Ensure example_transform function runs without errors
    assert example_transform(df) is None, "example_transform should not return anything"


def test_query(spark):
    """Test the query function"""
    df = load_data(spark, "data/WorldsBestRestaurants.csv")
    sample_query = """
    SELECT `Restaurant`, Country, Rank
    FROM BestRestaurants
    WHERE Rank < 20
    ORDER BY Rank ASC
    """
    # Ensure query function runs without errors
    assert (
        query(spark, df, sample_query, "BestRestaurants") is None
    ), "query should not return anything"
