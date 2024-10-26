"""
Analyzing YCombinator companies and what they have in common
"""
from loadDatasets.loadDatasets import DatabaseLoader
import pandas as pd
from confluent_kafka import Producer
from kafka.kafkaSingleton import KafkaProducerSingleton
from dataAnalytics import use_kafka_producer, getMostSuccessfulIndustriesFromKafka

def main():
    # Initialize the database loader
    loader = DatabaseLoader()
    loader.load_companies() 
    loader.load_industries()


    print (getMostSuccessfulIndustriesFromKafka('industries_topic'))


if __name__ == "__main__":
    main()

