# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

# Create a Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("SparkApp001")\
                    .getOrCreate()

# Creating a Dataframe on a CSV File
filePath = "./csvFiles/inputFiles/operations_management.csv"
df_data = spark.read.format("csv")\
                    .option("inferSchema", "True")\
                    .option("header", "True")\
                    .option("path", filePath)\
                    .load()

# Explore the dataframe
df_data.printSchema()
df_data.show(10)

# Process the dataframe and explore it
df_data_processed = df_data.select("industry", "value")\
                           .filter((func.col("value")>500) & (func.col("industry")!="total"))\
                           .orderBy(func.desc("value"))

df_data_processed.printSchema()
df_data_processed.show(10)

# Create view on the processed dataframe
df_data_processed.createOrReplaceTempView("processedData")

# Query the view using Spark SQL
spark.sql("""SELECT * 
             FROM processedData
             WHERE value = "total"
             ORDER BY value desc
          """)\
      .show(10)

