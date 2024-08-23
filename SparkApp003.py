# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

# Create a Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("SparkApp003")\
                    .getOrCreate()

# Create the schema of the dataframe
df_schema = StructType([\
                        StructField("UserId", IntegerType(), False),
                        StructField("Name", StringType(), True),
                        StructField("Age", IntegerType(), True),
                        StructField("FriendsId", IntegerType(), True),
                    ])

# Read the Dataframe from a CSV File
inputFilePath = "./csvFiles/inputFiles/fakefriends.csv"
df_data = spark.read.format('csv')\
                    .schema(df_schema)\
                    .option("inferSchema","False")\
                    .option("path", inputFilePath)\
                    .load()

# Explore the dataframe
df_data.printSchema()
df_data.show(10)

# Process the dataframe
df_data_processed = df_data.select(df_data.UserId, df_data.Name, df_data.Age, df_data.FriendsId)\
                           .where(df_data.Age < 30)\
                           .withColumn("Insert_ts", func.current_timestamp())\
                           .orderBy(df_data.UserId)\
                           .cache()

# Write the dataframe into a CSV file
outputFolderPath = "./csvFiles/outputFiles/op"
# df_data_processed.write.format("csv")\
#                        .mode("overwrite")\
#                        .option("path", outputFolderPath)\
#                        .partitionBy("Age")\
#                        .save()

# Write the dataframe into a parquet file
# df_data_processed.write.format("parquet")\
#                        .mode("overwrite")\
#                        .option("path", outputFolderPath)\
#                        .partitionBy("Age")\
#                        .save()

# Write the dataframe into a JSON file
df_data_processed.write.format("json")\
                       .mode("overwrite")\
                       .option("path", outputFolderPath)\
                       .partitionBy("Age")\
                       .save()
