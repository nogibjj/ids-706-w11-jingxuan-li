![image](https://github.com/user-attachments/assets/f1edc01e-c6f3-4078-a166-5e8116db3ec8)[![CI](https://github.com/nogibjj/ids-706-w11-jingxuan-li/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/ids-706-w11-jingxuan-li/actions/workflows/cicd.yml)
### Data Processing with PySpark
#Data Analysis Pipeline with Databricks

## Overview
This project demonstrates a Databricks pipeline for managing and analyzing restaurant data using Spark. The pipeline is broken into three steps:

1. **Extract**: Reads raw data from a publicly accessible CSV file.
2. **Load and Transform**: Processes the data, including loading and transformations like categorizing restaurants and saving results in Delta format.
3. **Query**: Performs SQL queries to analyze the transformed data.

The workflow is automated using **Databricks Jobs** and **GitHub Actions** for CI/CD.

## Databricks Setup
Follow these steps to set up and run the pipeline in Databricks:

### 1. Setting Up a Cluster
- Navigate to the **Compute** section in Databricks.
- Click **Create Cluster** and configure it with default settings.
- Start the cluster.

### 2. Connecting to GitHub
- Navigate to the **Workspace** tab.
- Create a new Git folder and link it to your repository:
  - Example Repository: `https://github.com/<your-repo>.git`

### 3. Installing Required Libraries
- **pyspark**: Pre-installed in Databricks clusters.
- Ensure any additional libraries are installed via `requirements.txt` or manually in the Databricks cluster.

### 4. Creating Jobs
- Navigate to the **Jobs** tab and create jobs for each pipeline step:
  - **Extract**: Runs the `extract.py` script.
  - **load_and_Transform**: Runs the `load.py` script.
  - **Query**: Runs the `query.py` script.
     ![image](https://github.com/user-attachments/assets/4d49c7ed-e1e3-488f-adb2-766e2c4661a9)


- **Task Dependencies**:
  - Set `Transform` to depend on `Extract`.
  - Set `Query` to depend on `load_and_Transform`.

---

## Data Source and Sink

### Data Source
The pipeline uses a publicly accessible CSV file:
- **URL**: `https://raw.githubusercontent.com/nogibjj/ids-706-w11-jingxuan-li/main/data/WorldsBestRestaurants.csv`

### Data Sink
The outputs of each pipeline stage are stored in **Delta format** for scalability:
- **Extracted Data**: `dbfs:/FileStore/ids-706-w11-jingxuan-li/WorldsBestRestaurants.csv`
- **Transformed Data**: `dbfs:/FileStore/ids-706-w11-jingxuan-li/BestRestaurants`

---

## Pipeline Workflow

### **Pipeline Overview**
This project follows an **ETL (Extract, Transform, Load)** process to analyze restaurant data:

1. **Extract**:
   - Reads data from the source URL.
   - Saves raw data in Delta format at `dbfs:/FileStore/ids-706-w11-jingxuan-li/WorldsBestRestaurants.csv`.

2. **Transform**:
   - Categorizes restaurants (e.g., based on country or rank).
   - Saves transformed data in Delta format at `dbfs:/FileStore/ids-706-w11-jingxuan-li/BestRestaurants`.

3. **Query**:
   - Executes SQL queries to filter and group the transformed data for analysis.

### **Task Dependencies**
Each step is executed sequentially:
- `Transform` depends on `Extract`.
- `Query` depends on `Transform`.

This ensures that downstream tasks operate on consistent data outputs from upstream steps.

---

## CI/CD Setup

### **Workflow Configuration**
The CI/CD pipeline is implemented using GitHub Actions and performs the following steps:
1. Installs required dependencies.
2. Lints and formats the code.
3. Runs tests to validate functionality.
4. Sets up environment variables for secure Databricks access.
5. Executes the Databricks jobs.

### **Environment Setup in GitHub**
Secrets are configured in GitHub to securely connect to Databricks:

1. Go to your GitHub repository.
2. Navigate to **Settings > Secrets and variables > Actions**.
3. Add the following secrets:
   - `SERVER_HOSTNAME`: Databricks server hostname.
   - `ACCESS_TOKEN`: Personal access token for Databricks.

---

## How to Run Locally
1. Clone the repository:
   ```bash
   git clone https://github.com/<your-repo>.git
   cd <your-repo>
   ```
2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Add a `.env` file with the following content:
   ```
   SERVER_HOSTNAME=https://<databricks-instance>.azuredatabricks.net
   ACCESS_TOKEN=<your-access-token>
   ```

5. Run the pipeline steps:
   - **Extract**:
     ```bash
     python extract.py
     ```
   - **Transform**:
     ```bash
     python load.py
     ```
   - **Query**:
     ```bash
     python query.py
     ```

---
