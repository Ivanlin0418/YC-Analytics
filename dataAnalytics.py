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
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", f"{topic}") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false")  \
            .load() \
            .selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), SCHEMA[topic.upper()]).alias("data")) \
            .select("data.*")
        
        return df



    def getMostSuccessfulCompaniesFromKafka(self, top_n=10):
        """
        Consumes messages from a Kafka topic and returns the top N most successful companies.
        
        @param top_n: Number of top companies to return. Defaults to 5.
        @return: List of top N companies sorted by success metric.
        """
        spark = self.spark

        # Read companies data from Kafka as a streaming DataFrame
        companies_df = self.subscribeToKafkaTopic("companies")

        # Cast teamSize to integer
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        query = companies_df.writeStream \
            .format("parquet") \
            .option("path", "/tmp/companies") \
            .option("checkpointLocation", "/tmp/checkpoints/companies") \
            .outputMode("append") \
            .start()

        # Wait for the streaming query to initialize
        query.awaitTermination(10)

        # Read the data back from the parquet sink
        companies_df = spark.read.parquet("/tmp/companies")

        # Create a temporary view
        companies_df.createOrReplaceTempView("companies")

        # filter to avoid SQL injection
        filtered_df = companies_df.filter((col("status") == "Acquired") & (col("teamSize").isNotNull())) \
                                  .select("name", col("teamSize").cast("int").alias("teamSize"), "batch")

        # Group by name, teamSize, and batch, then order by teamSize in descending order and limit the results
        result_df = filtered_df.groupBy("name", "teamSize", "batch") \
                               .count() \
                               .orderBy(col("teamSize").desc()) \
                               .limit(top_n)

        # Collect the result into a Pandas DataFrame
        result_pd = result_df.toPandas()

        # Plot the result using Plotly
        fig = px.bar(result_pd, x='name', y='teamSize', title='Top N Most Successful Companies by Team Size')

        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, 'most_successful_companies.html')

        # Save the plot to the same directory as the script
        fig.write_html(file_path)
        print(f"Plot saved to {file_path}")

        # Stop the streaming query
        query.stop()

    def getMostSuccessFoundersAndTheirCompaniesFromKafka(self, top_n=10):
        # Subscribe to Kafka topics for companies and founders
        companies_df = self.subscribeToKafkaTopic("companies")
        founders_df = self.subscribeToKafkaTopic("founders")

        # Cast teamSize to integer in companies DataFrame
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        # Write the streaming DataFrames to parquet sinks
        query_companies = self.writeCompaniesStream()
        query_founders = self.writeFoundersStream() 


        # Read the data back from the parquet sink after ensuring it's written
        companies_df = self.spark.read.parquet("/tmp/companies")
        founders_df = self.spark.read.parquet("/tmp/founders")

        # Filter acquired companies with valid team size
        filtered_companies_df = companies_df.filter((col("status") == "Acquired") & (col("teamSize").isNotNull())) \
                                            .select("name", col("teamSize").alias("teamSize"), "batch")

        # Join the filtered companies DataFrame with founders DataFrame on top_company field
        result_df = filtered_companies_df.join(founders_df, filtered_companies_df.name == founders_df["top_company"]) \
                                        .select("first_name", "last_name", "top_company", "teamSize", "batch") \
                                        .orderBy(col("teamSize").desc()) \
                                        .limit(top_n)

        # Collect the result into a Pandas DataFrame for plotting
        result_pd = result_df.toPandas()

        # Plot the result using Plotly
        fig = px.bar(result_pd, x='top_company', y='teamSize', color='first_name', 
                    title='Top N Most Successful Founders and Their Companies by Team Size')

        # Get the directory of the current script and save the plot as an HTML file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, 'most_successful_founders_companies.html')
        
        fig.write_html(file_path)
        print(f"Plot saved to {file_path}")

        # Stop the streaming queries after processing
        query_companies.stop()
        query_founders.stop()

        return result_df

    def writeCompaniesStream(self):
        # Subscribe to Kafka topic for companies
        companies_df = self.subscribeToKafkaTopic("companies")

        # Cast teamSize to integer
        companies_df = companies_df.withColumn("teamSize", companies_df["teamSize"].cast("int"))

        # Write the streaming DataFrame to a parquet sink
        query_companies = companies_df.writeStream \
            .format("parquet") \
            .option("path", "/tmp/companies") \
            .option("checkpointLocation", "/tmp/checkpoints/companies") \
            .outputMode("append") \
            .start()

        # Wait for the streaming query to initialize
        query_companies.awaitTermination(10)

        return query_companies

    def writeFoundersStream(self):
        # Subscribe to Kafka topic for founders
        founders_df = self.subscribeToKafkaTopic("founders")

        # Write the streaming DataFrame to a parquet sink
        query_founders = founders_df.writeStream \
            .format("parquet") \
            .option("path", "/tmp/founders") \
            .option("checkpointLocation", "/tmp/checkpoints/founders") \
            .outputMode("append") \
            .start()

        # Wait for the streaming query to initialize
        query_founders.awaitTermination(10)

        return query_founders
