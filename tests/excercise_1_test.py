from pyspark.sql import SparkSession
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
    pass