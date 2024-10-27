from pyspark.sql import SparkSession

class SparkSessionSingleton:
    _instance = None

    @classmethod
    def get_instance(cls, app_name="YC_Analysis", master="local[*]"):
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName(app_name) \
                .master(master) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
                .getOrCreate()
        return cls._instance
