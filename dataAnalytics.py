from kafka.kafkaSingleton import KafkaProducerSingleton, KafkaConsumerSingleton
from spark.sparkSingleton import SparkSessionSingleton
from pyspark.sql.functions import col, from_json
from spark.sparkSchemas import *
import pandas as pd
import plotly.express as px
import os

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


SCHEMA = {
    "BADGES": BADGES_SCHEMA,
    "COMPANIES": COMPANY_SCHEMA,
    "FOUNDERS": FOUNDERS_SCHEMA,
    "INDUSTRIES": INDUSTRIES_SCHEMA,
    "REGIONS": REGIONS_SCHEMA,
    "SCHOOLS": SCHOOLS_SCHEMA,
    "TAGS": TAGS_SCHEMA,
}


class DataAnalytics:
    def __init__(self):
        sparkSingleton = SparkSessionSingleton()
        kafkaProducerSingleton = KafkaProducerSingleton()
        kafkaConsumerSingleton = KafkaConsumerSingleton()  # Correct instantiation
        self.spark = sparkSingleton.get_instance(app_name="CompanyAnalysis")
        self.producer = kafkaProducerSingleton.get_producer()
        self.consumer = kafkaConsumerSingleton.get_consumer()  # Use the instance to get the consumer

    def useKafkaProducer(self): #this is how you add data to kafka
        kafkaProducerSingleton = KafkaProducerSingleton()
        producer = kafkaProducerSingleton.get_producer()
        producer.produce('some_topic', value='some_message') #topic, message
        producer.flush() 


    def subscribeToKafkaTopic(self, topic):
        """
        Subscribes to a Kafka topic and returns a DataFrame.
        
        @param topic: Name of the Kafka topic to subscribe to.
        @return: DataFrame containing the data from the Kafka topic.
        """
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic+"_topic") \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load() \
                .selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), SCHEMA[topic.upper()]).alias("data")) \
                .select("data.*")
            return df
        except Exception as e:
            print(f"An error occurred while subscribing to the Kafka topic {topic}: {e}")
            return None
        
    def getKafkaTopicList(self):
        """
        Returns a list of Kafka topics.
        
        @return: List of Kafka topics.
        """
        try:
            topics = self.consumer.list_topics().topics
            print (self.producer.list_topics().topics)
            return list(topics.keys())
        except Exception as e:
            print(f"An error occurred while getting the list of Kafka topics: {e}")
            return None
        

    def getMostSuccessfulCompaniesFromKafka(self, top_n=10):
        """
        Consumes messages from a Kafka topic and returns the top N most successful companies by team size.
        
        @param top_n: Number of top companies to return. Defaults to 10.
        @return: List of top N companies sorted by success metric.
        """
        spark = self.spark

        # Read companies data from Kafka as a streaming DataFrame
        companies_df = self.subscribeToKafkaTopic("companies")
        if companies_df is None:
            print("Failed to subscribe to the Kafka topic 'companies'.")
            return None

        # Cast teamSize to integer
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        # Filter acquired companies with non-null teamSize
        filtered_df = companies_df.filter((col("status") == "Acquired") & (col("teamSize").isNotNull()))

        # Group by name, teamSize, and batch, then order by teamSize in descending order and limit the results
        result_df = filtered_df.groupBy("name", "teamSize", "batch") \
                            .count() \
                            .orderBy(col("teamSize").desc()) \
                            .limit(top_n)

        # Write the result to an in-memory table (for testing purposes)
        query = result_df.writeStream \
            .format("memory") \
            .queryName("top_companies") \
            .outputMode("complete") \
            .start()

        query.awaitTermination(10)
        sqlQuery = """SELECT * 
                   FROM top_companies 
                   ORDER BY teamSize DESC 
                   LIMIT :top_n"""
        result_static_df = spark.sql(sqlQuery, args={"top_n": top_n})


        result_pd = result_static_df.toPandas()

        # Plot using Plotly
        fig = px.bar(result_pd, x='name', y='teamSize', title=f'Top {top_n} Most Successful Companies by Team Size')
        fig.show()

        # Stop the streaming query
        query.stop()

        return result_pd

    
    def getMostSuccessFoundersAndTheirCompaniesFromKafka(self, top_n=10):
        """
        Consumes messages from Kafka topics for companies and founders,
        joins them, and returns the top N most successful founders and their companies.
        
        @param top_n: Number of top companies/founders to return. Defaults to 10.
        @return: List of top N founders and their companies sorted by team size.
        """
        spark = self.spark

        # Subscribe to Kafka topics for companies and founders
        companies_df = self.subscribeToKafkaTopic("companies")
        founders_df = self.subscribeToKafkaTopic("founders")

        if companies_df is None or founders_df is None:
            print("Failed to subscribe to one or more Kafka topics.")
            return None

        # Cast teamSize to integer in companies DataFrame
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        # Filter acquired companies with valid team size
        filtered_companies_df = companies_df.filter((col("status") == "Acquired") & (col("teamSize").isNotNull())) \
                                            .select("name", col("teamSize").alias("teamSize"), "batch")

        # Join the filtered companies DataFrame with founders DataFrame on 'top_company' field
        result_df = filtered_companies_df.join(
            founders_df,
            filtered_companies_df["name"] == founders_df["top_company"]
        ).select(
            "first_name", "last_name", "top_company", "teamSize", "batch"
        ).orderBy(
            col("teamSize").desc()
        ).limit(top_n)

        # Write the result to an in-memory table (for testing purposes) using append mode
        query = result_df.writeStream \
            .format("memory") \
            .queryName("top_founders_companies") \
            .outputMode("append") \
            .start()

        # Wait for the stream to process some data (adjust time as needed)
        query.awaitTermination(10)

        # Query the in-memory table for results
        sqlQuery = """
                   SELECT * 
                   FROM top_founders_companies
                   ORDER BY teamSize DESC 
                   LIMIT :top_n"""
        result_static_df = spark.sql(sqlQuery, args={"top_n": top_n})

        # Collect results into Pandas DataFrame for visualization
        result_pd = result_static_df.toPandas()

        # Plot using Plotly
        fig = px.bar(result_pd, x='top_company', y='teamSize', color='first_name', 
                    title=f'Top {top_n} Most Successful Founders and Their Companies by Team Size')  
        fig.show()

        query.stop()
        return result_static_df


