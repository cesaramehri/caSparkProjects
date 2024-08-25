### Read data from a MongoDB Table ###

# Import Packages
from pyspark.sql import SparkSession, Row, functions

# Main function
if __name__ == "__main__":
    # Create s Spark Session
    spark = SparkSession.builder\
                        .appName("MongoDBIntegration")\
                        .getOrCreate()

    # Read data from MongoDB table into a Dataframe
    readUser = spark.read\
                     .format("com.mongodb.spark.sql.DefaultSource")\
                     .option("uri","mongodb://127.0.0.1/movies.user")\
                     .load()
    
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
