from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from excercises.excercise_1 import transform_employees

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("EmployeeTest  ") \
    .getOrCreate()

def test_transform_employees():
    test_data = [
    (1, "Alice", 60000, "IT"),
    (2, "Bob", 45000, "HR"),
    (3, "Charlie", 80000, "IT"),
    (4, "David", 50000, "Sales"),
    (5, "Eva", 90000, "IT")
    ]

    columns = ["id", "name", "salary", "dept"]

    input_df = spark.createDataFrame(test_data, columns)

    result = transform_employees(input_df)

    expected_data = [
        (1, "Alice", 60000, "MEDIUM"),
        (3, "Charlie", 80000, "HIGH"),
        (5, "Eva", 90000, "HIGH")
    ]

    expected_columns = [
        "id", 
        "name",
        "salary",
        "salary_category"
    ]

    expected = spark.createDataFrame(expected_data, expected_columns)

    assert result.collect() == expected.collect()

def test_salary_50000_is_excluded():
    test_data = [
        (1, "Alice", 60000, "IT"),
        (2, "Bob", 45000, "HR"),
        (3, "Charlie", 80000, "IT"),
        (4, "David", 50000, "Sales"),
        (5, "Eva", 90000, "IT")
    ]

    columns = ["id", "name", "salary", "dept"]

    input_df = spark.createDataFrame(test_data, columns)

    result = transform_employees(input_df)

    assert result.filter(col("id") == 4).count() == 0

def test_salary_50001_is_medium():
    test_data = [
        (1, "Alice", 50001, "IT") 
    ]

    columns = ["id", "name", "salary", "dept"]

    input_df = spark.createDataFrame(test_data, columns)

    result = transform_employees(input_df)

    expected_data = [
        (1, "Alice", 50001, "MEDIUM")
 
    ]

    expected_columns = ["id", "name", "salary", "salary_category"]

    expected = spark.createDataFrame(expected_data, expected_columns)

    assert result.collect() == expected.collect()

def test_salary_80000_is_high():
    test_data = [
        (3, "Janice", 80000, "Sales") 
    ]

    columns = ["id", "name", "salary", "dept"]

    input_df = spark.createDataFrame(test_data, columns)

    result = transform_employees(input_df)

    expected_data = [
        (3, "Janice", 80000, "HIGH")
 
    ]

    expected_columns = ["id", "name", "salary", "salary_category"]

    expected = spark.createDataFrame(expected_data, expected_columns)

    assert result.collect() == expected.collect()


def test_salary_79999_is_medium():
    test_data = [
        (3, "Kerry", 79999, "Sales") 
    ]

    columns = ["id", "name", "salary", "dept"]

    input_df = spark.createDataFrame(test_data, columns)

    result = transform_employees(input_df)

    expected_data = [
        (3, "Kerry", 79999, "MEDIUM")
 
    ]

    expected_columns = ["id", "name", "salary", "salary_category"]

    expected = spark.createDataFrame(expected_data, expected_columns)

    assert result.collect() == expected.collect()

def test_null_salary():
    test_data = [
            (3, "Kerry", None, "Sales") 
        ]
    
    schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept", StringType(), True)
])

    input_df = spark.createDataFrame(test_data, schema)

    result = transform_employees(input_df)

    assert result.count() == 0