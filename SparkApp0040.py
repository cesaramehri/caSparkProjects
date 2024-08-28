# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
import pandas as pd

# Create a Spark Session
spark = SparkSession.builder\
                    .master("local")\
                    .appName("SparkApp004")\
                    .getOrCreate()

# UDF
def cubed(a:pd.Series) -> pd.Series:
    return a * a * a

# Create a Pandas UDF (to speed up PySpark UDFs)
cubed_udf = pandas_udf(cubed,returnType=LongType())
x = pd.Series([1, 2, 3, 4, 5])
print("cubed_x = \n", cubed(x))

# Execute UDF as a Spark vectorized UDF
df = spark.range(1,6)
df.select("id", cubed_udf(col("id"))).show() # -- Problem here!!