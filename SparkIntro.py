# Import Packages
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("SparkIntro")\
                    .getOrCreate()
print(spark)

# Define the data and schema
data = [('James','','Smith','1991-04-01','M',3000),
        ('Michael','Rose','','2000-05-19','M',4000),
        ('Robert','Julius','Williams','1978-09-05','M',4000),
        ('Maria','Anne','James','1967-12-01','F',4000),
        ('Jen','Mary','Brown','1980-02-17','F',-1),]
schema = ["firstname","middlename","lastname","dob","gender","salary"]

# Create a dataframe for the defined data/schema
df = spark.createDataFrame(data=data, schema=schema)
print(df.printSchema())
