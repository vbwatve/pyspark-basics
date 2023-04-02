from pyspark.sql import SparkSession


def read_csv(file_path):
    # Builder class for creation of Spark session.
    spark = SparkSession.builder.\
        master("local[*]").\
        appName('Pyspark Basics').\
        getOrCreate()

    return spark.read.format('csv').option('header', True).load("h1b_data.csv")

