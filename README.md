[![CI](https://github.com/nogibjj/ids-706-w10-jingxuan-li/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/ids-706-w10-jingxuan-li/actions/workflows/cicd.yml)
### Data Processing with PySpark

#### Project Overview
This project involves data processing using PySpark to analyze and transform a dataset of the world's best restaurants.  The project includes functions for loading data, performing transformations, and running SQL queries using Spark SQL.

#### File Structure
- `mylib/lib.py`: Contains the main functions for data processing, including data loading, describing, transformations, and querying.
- `main.py`: A script that imports and executes the functions defined in `mylib/lib.py` to perform data analysis and transformations.
- `data/WorldBestRestaurants.csv`: csv file
- `test_main.py`: A `pytest`-based test suite for validating the functionality in `mylib/lib.py`.
- `pyspark_output.md`: generated output report for the main.py

#### Functionality
1. **Data Processing Functionality**
   - **Loading Data**: The `load_data()` function loads a CSV file into a PySpark DataFrame with a predefined schema to ensure correct data types and structure.
   - **Descriptive Statistics**: The `describe()` function computes and logs basic statistics (e.g., mean, standard deviation) for each numerical column in the DataFrame.
   - **Logging**: The `log_output()` function writes the operation details and output to a markdown file, `pyspark_output.md`, for review.

2. **Use of Spark SQL and Transformations**
   - **SQL Queries**: The `query()` function allows you to run SQL queries on the DataFrame by creating a temporary SQL view. The output is logged and displayed.
   - **Data Transformations**: The `example_transform()` function performs data transformations using conditional logic to categorize restaurants based on their country and ranking.
   - **Spark SQL Example**:
     ```python
     sample_query = """
     SELECT `Restaurant`, Country, Rank
     FROM BestRestaurants
     WHERE Rank < 20
     ORDER BY Rank ASC
     """
     query(spark, df, sample_query, "BestRestaurants")
     ```
     This query retrieves restaurants ranked in the top 20, ordered by their rank.

#### Usage
1. **Setup**:
   - Use GitHub Codespaces to open
   - wait for environment to be installed

2. **Installation**

Install the required packages using `pip`:
```bash
make install
```

3. **Run the Main Script**:
   ```bash
   python main.py
   ```
   This will load the data, print descriptive statistics, apply transformations, and run an SQL query.

4. **Run Tests**:
   Execute the test suite to validate the functions:
   ```bash
   make test
   ```
   ![image](https://github.com/user-attachments/assets/fe540995-fdeb-407f-b409-e0f7f1080ccf)


#### Project Components
- **Data Loading**: Reads the CSV data with a structured schema using `load_data()`.
- **Descriptive Statistics**: The `describe()` function provides an overview of data distributions.
- **Transformations**: The `example_transform()` function categorizes rows based on specific conditions.
- **SQL Queries**: The `query()` function runs SQL commands to filter and sort the data.

#### Example Output
- **Log File**: The `pyspark_output.md` will contain the outputs of each operation in markdown format for easy review.
- **Console**: The console will display outputs using `.show()` for immediate feedback.
