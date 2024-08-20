# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

# Create a Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("FirstApp")\
                    .getOrCreate()

# Define the schema for your Dataframe
df_schema = StructType([StructField("userID", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                        StructField("friends", IntegerType(), True)
                        ])

# Creating a Dataframe on a CSV File
df_data = spark.read.format("csv")\
                    .schema(df_schema)\
                    .option("path", "fakefriends.csv")\
                    .load()

# Print schema
print(df_data.printSchema())


