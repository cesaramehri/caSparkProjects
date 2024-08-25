### Read data from a Cassandra Table and write it into a new Cassandra table ###

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
    spark = SparkSession.builder\
                        .appName("CassandraIntegration")\
                        .config("spark.cassandra.connection.host", "127.0.0.1")\
                        .getOrCreate()

    # Buil RDD on top of users data file
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/cassandra/movies.user")

    # Create new RDD by providing the schema
    users = lines.map(parseInput)

    # Convert RDD -> Dataframe
    usersDF = spark.createDataFrame(users)

    # Write the data into Cassandra
    usersDF.write.format("org.apache.spark.sql.cassandra")\
                 .mode("append")\
                 .options(keyspace = "moviesdata", table = "users")\
                 .save()