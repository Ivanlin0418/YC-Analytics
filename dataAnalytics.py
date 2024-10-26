from kafka.kafkaSingleton import KafkaProducerSingleton, KafkaConsumerSingleton
from spark.sparkSingleton import SparkSessionSingleton
import json
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def use_kafka_producer(): #this is how you add data to kafka
    kafka_producer_singleton = KafkaProducerSingleton()
    producer = kafka_producer_singleton.get_producer()

    producer.produce('some_topic', value='some_message')
    producer.flush() 


def getMostSuccessfulIndustriesFromKafka(topic, top_n=5):
    """
    Consumes messages from a Kafka topic and returns the top N most successful industries.

    :param topic: The Kafka topic to consume messages from.
    :param top_n: Number of top industries to return.
    :return: List of top N industries sorted by success metric.
    """
    kafka_consumer_singleton = KafkaConsumerSingleton()
    consumer = kafka_consumer_singleton.get_consumer()

    industry_data = {}

    try:
        # Subscribe to the specified topic
        consumer.subscribe([topic])

        # Consume messages from the topic
        while True:
            message = consumer.poll(timeout=1.0)  # Poll for messages with a timeout

            if message is None:
                print("No message received, continuing to poll...")
                continue  # No message received, continue polling

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            try:
                # Decode and load the message value
                industry_info = json.loads(message.value().decode('utf-8'))
                print("Message received:", industry_info)  # Debug: Print the entire message
                print("Keys in message:", industry_info.keys())  # Debug: Print the keys

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

            if len(industry_data) >= 1000:
                break

    finally:
        # Close the consumer
        consumer.close()

    # Sort the industries by success metric in descending order
    sorted_industries = sorted(industry_data.items(), key=lambda x: x[1], reverse=True)

    # Return the top N industries
    return sorted_industries[:top_n]



def getMostSuccessfulCompaniesFromKafka(topic, top_n=5):
    """
    Consumes messages from a Kafka topic and returns the top N most successful companies.
    
    @param topic: The Kafka topic to consume messages from.
    @param top_n: Number of top companies to return. Defaults to 5.
    @return: List of top N companies sorted by success metric.
    """
    spark = SparkSessionSingleton.get_instance(app_name="CompanyAnalysis")

    # Define the schema for the JSON data
    schema = StructType([
        StructField("company_name", StringType(), True),
        StructField("success_metric", FloatType(), True)
    ])

    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .load()

    # Convert the binary value to string and parse JSON
    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Sort the data by 'success_metric' in descending order
    sorted_df = df.orderBy(col('success_metric').desc())

    # Start the streaming query to show the top N companies
    query = sorted_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()
