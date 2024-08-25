### Read data from a MongoDB Table and write it into a new MongoDB table ###

# Import Packages
from pyspark.sql import SparkSession, Row, functions

# Extract fields using split function
def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]),\
               age = int(fields[0]),\
               gender = int(fields[0]),\
               occupation = int(fields[0]),\
               zip = fields[4]
               )

# Main function
if __name__ == "__main__":
    # Create s Spark Session
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Buil RDD on top of users data file
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")

    # Create new RDD by providing the schema
    users = lines.map(parseInput)

    # Convert RDD -> Dataframe
    usersDF = spark.createDataFrame(users)

    # Write the data into MongoDB
    usersDF.write.format("com.mongodb.spark.sql.DefaultSource")\
                 .option("uri","mongodb://127.0.0.1/moviesdata.users")\
                 .mode("append")\
                 .save()