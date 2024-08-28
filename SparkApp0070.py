# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("sparkStream")\
                    .getOrCreate()

# Define Input Sources/Mode, Read Data from a Stream generator (NCAT tool) into a dataframe
lines = ( spark.readStream\
               .format("socket")\
               .option("host", "localhost")\
               .option("port", 9999)\
               .load()
)

# Data Processing: Transform Data (Splitting, Counting)
# 1. Split the lines into single words (word1, word 2, ...)
words = lines.select(split(col("value"), " ").alias("word"))

# 2. Count the number words by word, as a result you obtain words_cnt = set(word, word frequency) 
words_cnt = words.groupBy("word").count()

# Define Output Sink/Mode
checkpointDir = "C:/checkpoint/"

# Stream data out to the sink
streamingQuery = (words_cnt.writeStream\
                           .format("console")\
                           .outputMode("complete")\
                           .trigger(processingTime = "1 second")
                           .option("checkpointLocation", checkpointDir)
                           .start()
)
streamingQuery.awaitTermination()
