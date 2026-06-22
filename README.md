# CSI-Assignment-5
# 🚀 Apache Spark Data Processing Pipeline

## 📌 Project Overview

This project demonstrates the core concepts of **Apache Spark DataFrames** through a complete data processing workflow. The implementation covers data cleaning, transformation, schema management, filtering, aggregation, and analytical operations on a real-world retail dataset.

The objective is to understand how Spark processes large-scale data efficiently using distributed computing and in-memory execution while building a robust ETL-style data processing pipeline.

---

## 🎯 Objectives

- Understand limitations of MapReduce and advantages of Apache Spark.
- Learn Spark DataFrame concepts and immutability.
- Perform data cleaning and preprocessing.
- Handle null values and inconsistent records.
- Apply filtering and transformation operations.
- Execute aggregation and group-based analytics.
- Understand shuffle operations and wide transformations.
- Modify schemas and data types.
- Build a complete end-to-end Spark processing pipeline.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|------------|----------|
| Python | Programming Language |
| Apache Spark | Distributed Data Processing |
| PySpark | Spark API for Python |
| Jupyter Notebook | Development Environment |
| CSV Dataset | Input Data Source |

---

## 📂 Project Structure

```text
Apache-Spark-Data-Processing/
│
├── Assignment-5.ipynb
├── Sample-Superstore.csv
├── results.csv
├── README.md
│
└── Output/
    ├── Cleaned_Data
    ├── Aggregation_Results
    └── Business_Insights
```

---

## 📊 Dataset Information

The dataset contains retail sales information including:

- Order Details
- Customer Information
- Product Categories
- Regional Sales Data
- Quantity, Sales, and Profit Metrics

This data is processed using Spark DataFrames to perform scalable analytics and business reporting.

---

# Apache Spark Fundamentals

## Why Spark Over MapReduce?

| MapReduce | Apache Spark |
|------------|-------------|
| Disk-Based Processing | In-Memory Processing |
| Slower Execution | Faster Execution |
| Multiple Read/Write Operations | Reduced Disk I/O |
| Complex Development | Easy-to-Use APIs |
| Batch Processing Only | Batch + Streaming Support |

### Advantages of Spark

- Faster Processing
- Fault Tolerance
- Distributed Computing
- Scalable Architecture
- In-Memory Analytics

---

# Data Cleaning Operations

## Remove Duplicate Records

```python
df_clean = df.dropDuplicates()
```

### Purpose

Removes duplicate rows to ensure data consistency and improve result accuracy.

---

## Handle Null Values

### Remove Rows Containing Null Values

```python
df_clean = df.na.drop()
```

### Fill Null Values

```python
df_clean = df.na.fill(0)
```

### Purpose

- Improves data quality
- Prevents aggregation errors
- Ensures accurate analysis

---

## Data Consistency Checks

```python
from pyspark.sql.functions import col

df.filter(
    (col("Sales").isNull()) |
    (col("Profit").isNull())
).show()
```

### Purpose

Detects missing or inconsistent records before processing.

---

# Data Transformation Operations

## Filtering Records

### Filter Sales Greater Than 500

```python
df.filter(df.Sales > 500).show()
```

### Filter Quantity Greater Than 5

```python
df.filter(df.Quantity > 5).show()
```

### Multiple Conditions

```python
df.filter(
    (df.Quantity > 5) &
    (df.Profit > 100)
).show()
```

### Purpose

Extracts meaningful subsets of data for targeted analysis.

---

## Rename Columns

```python
df = df.withColumnRenamed(
    "Sales",
    "Total_Sales"
)
```

### Purpose

Improves schema readability and reporting clarity.

---

## Cast Data Types

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "Sales",
    col("Sales").cast("double")
)
```

### Purpose

Ensures numerical operations are performed correctly.

---

# Aggregation Operations

## Count Records

```python
df.count()
```

### Example Output

```text
9994
```

---

## Total Sales

```python
from pyspark.sql.functions import sum

df.select(
    sum("Sales")
).show()
```

### Example Output

```text
+------------+
|Total Sales |
+------------+
|2297200.86  |
+------------+
```

---

## Average Profit

```python
from pyspark.sql.functions import avg

df.select(
    avg("Profit")
).show()
```

### Example Output

```text
+------------+
|Avg Profit  |
+------------+
|28.66       |
+------------+
```

---

## Minimum and Maximum Sales

```python
from pyspark.sql.functions import min, max

df.select(
    min("Sales"),
    max("Sales")
).show()
```

### Example Output

```text
+---------+---------+
|MinSales |MaxSales |
+---------+---------+
|0.44     |22638.48 |
+---------+---------+
```

---

# GroupBy Analytics

## Total Sales by Region

```python
from pyspark.sql.functions import sum

df.groupBy("Region") \
  .agg(sum("Sales").alias("Total Sales")) \
  .show()
```

### Example Output

```text
+---------+------------+
|Region   |Total Sales |
+---------+------------+
|West     |725457.82   |
|East     |678781.24   |
|Central  |501239.89   |
|South    |391721.91   |
+---------+------------+
```

---

## Average Profit by Category

```python
from pyspark.sql.functions import avg

df.groupBy("Category") \
  .agg(avg("Profit").alias("Average Profit")) \
  .show()
```

### Example Output

```text
+---------------+---------------+
|Category       |Average Profit |
+---------------+---------------+
|Furniture      |8.69           |
|Office Supplies|20.32          |
|Technology     |78.75          |
+---------------+---------------+
```

---

## Aggregated Filtering

```python
from pyspark.sql.functions import sum

df.groupBy("Region") \
  .agg(sum("Sales").alias("TotalSales")) \
  .filter("TotalSales > 500000") \
  .show()
```

### Example Output

```text
+-------+-----------+
|Region |TotalSales |
+-------+-----------+
|West   |725457.82  |
|East   |678781.24  |
|Central|501239.89  |
+-------+-----------+
```

---

# Wide Transformations and Shuffle Operations

Wide transformations require data movement between partitions and trigger shuffle operations.

Examples:

```python
df.groupBy("Region").sum("Sales")
```

```python
df.join(other_df, "Customer ID")
```

### Key Observation

Shuffle operations are expensive because Spark redistributes data across executors. Efficient partitioning improves performance.

---

# End-to-End Processing Pipeline

```text
Load Dataset
      │
      ▼
Schema Validation
      │
      ▼
Data Cleaning
      │
      ▼
Null Handling
      │
      ▼
Transformations
      │
      ▼
Filtering
      │
      ▼
Aggregations
      │
      ▼
Business Insights
      │
      ▼
Export Results
```

---

# Key Learnings

### Spark Architecture

- Driver Program
- Executors
- Cluster Manager
- Distributed Processing

### DataFrames

- Immutable Structure
- Lazy Evaluation
- Catalyst Optimizer
- Efficient Query Execution

### Data Processing

- Data Cleaning
- Schema Management
- Transformations
- Aggregations
- GroupBy Operations

---

# Results and Insights

### Data Quality Improvements

- Duplicate records removed.
- Missing values handled successfully.
- Schema inconsistencies corrected.
- Data types standardized.

### Business Insights

- Regional sales performance analyzed.
- Category-wise profit trends identified.
- Aggregate metrics generated for reporting.
- Large-scale data processed efficiently using Spark.

---

# Future Enhancements

- Spark SQL Integration
- Real-Time Streaming with Kafka
- Delta Lake Implementation
- Databricks Deployment
- Snowflake Integration
- Power BI Dashboarding

---

# Learning Outcomes

By completing this project, the following concepts were successfully implemented:

✅ Apache Spark Fundamentals

✅ DataFrame Operations

✅ Data Cleaning Techniques

✅ Schema Management

✅ Filtering and Transformations

✅ Aggregation Functions

✅ GroupBy Operations

✅ Shuffle Operations

✅ End-to-End Data Processing Pipeline

---

# References

- Apache Spark Documentation
- PySpark API Documentation

---

## Author
**Pranjak Upadhyay [Poornima University]**

**Data Engineering & Apache Spark Practice Project**

Built to understand distributed data processing, DataFrame transformations, and scalable analytics using Apache Spark.
