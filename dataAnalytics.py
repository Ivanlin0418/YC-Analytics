from kafka.kafkaSingleton import KafkaProducerSingleton, KafkaConsumerSingleton
from spark.sparkSingleton import SparkSessionSingleton
from pyspark.sql.functions import col, from_json
from spark.sparkSchemas import *
import pandas as pd
import plotly.express as px

#------------SCHEMAS-------------
"""
badges ->          id,badge
companies ->       id,name,slug,website,smallLogoUrl,oneLiner,longDescription,teamSize,url,batch,status
founders ->        first_name,last_name,hnid,avatar_thumb,current_company,current_title,company_slug,top_company
industries ->      id,industry
prior_companies -> hnid,company
regions ->         id,region,country,address
schools ->         hnid,school,field_of_study,year
tags ->            id,tag
"""

#--------------------------------

class DataAnalytics:
    def __init__(self):
        spark_singleton = SparkSessionSingleton()
        kafka_producer_singleton = KafkaProducerSingleton()
        kafka_consumer_singleton = KafkaConsumerSingleton()  # Correct instantiation
        self.spark = spark_singleton.get_instance(app_name="CompanyAnalysis")
        self.producer = kafka_producer_singleton.get_producer()
        self.consumer = kafka_consumer_singleton.get_consumer()  # Use the instance to get the consumer

    def useKafkaProducer(self): #this is how you add data to kafka
        kafka_producer_singleton = KafkaProducerSingleton()
        producer = kafka_producer_singleton.get_producer()
        producer.produce('some_topic', value='some_message') #topic, message
        producer.flush() 

    def getMostSuccessfulCompaniesFromKafka(self, top_n=10):
        """
        Consumes messages from a Kafka topic and returns the top N most successful companies.
        
        @param top_n: Number of top companies to return. Defaults to 5.
        @return: List of top N companies sorted by success metric.
        """
        spark = self.spark

        # Read companies data from Kafka as a streaming DataFrame
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "companies_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), COMPANY_SCHEMA).alias("data")) \
            .select("data.*")

        # Cast teamSize to integer
        kafka_df = kafka_df.withColumn("teamSize", kafka_df["teamSize"].cast("int"))

        # Create a temporary view
        kafka_df.createOrReplaceTempView("companies")

        # Use SQL to perform the analytics
        sql_query = f"""
            SELECT name, CAST(teamSize AS INT) as teamSize, batch
            FROM companies
            WHERE status = 'Acquired'
            AND teamSize IS NOT NULL 
            GROUP BY name, teamSize, batch
            ORDER BY teamSize DESC
            LIMIT {top_n}
        """

        result_df = spark.sql(sql_query)

        # complete mode is used when you want to see the entire history of the data
        query = result_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query.awaitTermination()

    def getMostSuccessfulCompaniesFromKafkaBatch(self, top_n=5):
        spark = self.spark

        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "companies_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), COMPANY_SCHEMA).alias("data")) \
            .select("data.*")

        if kafka_df.count() == 0:
            print("No data available in Kafka topic.")
            return []

        kafka_df.createOrReplaceTempView("companies")

        sql_query = f"""
            SELECT acquired, name
            FROM companies
            ORDER BY acquired ASC
        """

        result_df = spark.sql(sql_query)
        result_df.show()

        top_companies = result_df.collect()
        return [(row['name'], row['acquired']) for row in top_companies]

    def getMostSuccessfulCompaniesWithFoundersFromKafka(self, top_n=10):
        """
        Consumes messages from Kafka topics and returns the top N most successful companies with their founders.
        
        @param top_n: Number of top companies to return. Defaults to 5.
        @return: List of top N companies with founders sorted by success metric.
        """
        # Get the Kafka consumer
        consumer = self.consumer
        spark = self.spark

        # Subscribe to the Kafka topic
        consumer.subscribe(["companies_topic"])

        # Read companies data from Kafka as a streaming DataFrame
        companies_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "companies_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), COMPANY_SCHEMA).alias("data")) \
            .select("data.*")

        # Cast teamSize to integer
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        # Create a temporary view for companies
        companies_df.createOrReplaceTempView("companies")

        # Read founders data from Kafka as a static DataFrame
        founders_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "founders_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), FOUNDERS_SCHEMA).alias("data")) \
            .select("data.*")

        # Create a temporary view for founders
        founders_df.createOrReplaceTempView("founders")

        # Use SQL to perform the join and analytics
        sql_query = f"""
            SELECT f.first_name, f.last_name, c.name as company_name, COUNT(*) as count
            FROM companies c
            JOIN founders f ON c.slug = f.company_slug
            GROUP BY f.first_name, f.last_name, c.name
            ORDER BY count DESC, c.acquired ASC
            LIMIT {top_n}
        """

        result_df = spark.sql(sql_query)

        # Use foreachBatch to process each micro-batch
        query = result_df.writeStream \
            .outputMode("complete") \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()




