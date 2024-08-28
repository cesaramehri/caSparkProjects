# Demo: Read data from a file and read it into a file #

# Import Packages
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Defining directories' paths
inputDir = "./demosFiles/streaming/input"
outputDir = "./demosFiles/streaming/output"
checkpointDir = "C:/checkpoint/"