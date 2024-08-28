# Demo: Read data from a file and Write it into a file #

# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Defining directories' paths
inputDir = "./demosFiles/streaming/input/"
outputDir = "./demosFiles/streaming/output/"
checkpointDir = "C:/checkpoint/"

# Create Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("sparkStream")\
                    .getOrCreate()

# Define the schema of the file
input_schema = StructType([ StructField("userID", IntegerType(), False),
                            StructField("name", StringType(), True),
                            StructField("age", IntegerType(), True),
                            StructField("friends", IntegerType(), True),
])

# Read the input stream into a dataframe
input_df = (spark.readStream\
                 .schema(input_schema)\
                 .csv(inputDir)
)

# Process the data
files_df_pr = input_df.select("name", "age")\
                      .where(input_df.age > 30)

# Write the output (notice we also add the checkpointDir so that in case of failure, the job restarts the that directory and not from the initial step)
streamingQuery = (files_df_pr.writeStream\
                             .format("csv")\
                             .outputMode("append")\
                             .option("path", outputDir)\
                             .option("header", True)\
                             .option("checkpointLocation", checkpointDir)\
                             .start()\
                             .awaitTermination(10)
)
