# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

# Create a spark session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("SparkApp002")\
                    .getOrCreate()
print(spark.version)

# Creating a Dataframe on a CSV File
filePath = "./demosFiles/batch/input/operations_management.csv"
df_data = spark.read.format("csv")\
                    .option("inferSchema", "true")\
                    .option("header", "true")\
                    .option("path", filePath)\
                    .load()

# Explore your dataframe
df_data.printSchema()
df_data.show(10)

# Create a view on your dataframe
df_data.createOrReplaceTempView("df_data_view")

# Create a new dataframe for the view
df2_data = spark.sql("""
                        SELECT * FROM df_data_view
                """)

# Explore the new dataframe
df2_data.show(10)

# Explore metadata -> show databases, tables, etc.
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())

# Drop View
spark.catalog.dropTempView("df_data_view")

