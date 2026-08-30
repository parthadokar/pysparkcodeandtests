from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("EmployeePractice") \
    .getOrCreate()

data = [
    (1, "Alice", 60000, "IT"),
    (2, "Bob", 45000, "HR"),
    (3, "Charlie", 80000, "IT"),
    (4, "David", 50000, "Sales"),
    (5, "Eva", 90000, "IT")
]

columns = ["id", "name", "salary", "dept"]

employees = spark.createDataFrame(data, columns)

# employees.show()

def transform_employees(df):
    df_result = df.withColumn(
    "salary_category",
     when(col('salary') >= 80000, "HIGH")
     .when(col('salary') > 50000, "MEDIUM")
     .otherwise("LOW")
    )

    df_result = df_result.filter(col('salary') > 50000).select("id", "name", "salary", "salary_category")

    return df_result
