### Read data from a Cassandra Table ###

# Import Packages
from pyspark.sql import SparkSession, Row, functions

# Main function
if __name__ == "__main__":
    # Create s Spark Session
    spark = SparkSession.builder\
                        .appName("CassandraIntegration")\
                        .config("spark.cassandra.connection.host", "127.0.0.1")\
                        .getOrCreate()

    # Read data from Cassandra table into a Dataframe
    readUser = spark.read\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode("append")\
                    .options(keyspace = "movies", table = "user")\
                    .save()
    
    # Create a view on the df 
    readUser.createOrReplaceTempView("user")

    # Explore the df
    readUser.printSchema()

    # Query the df using sql
    sqlDF = spark.sql("""
                        SELECT occupation, count(user_id) as cnt_usr
                        FROM user
                        GROUP BY occupation
                        ORDER BY cnt_user DESC
                      """)
    sqlDF.show()
